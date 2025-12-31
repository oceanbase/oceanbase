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
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_define.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/tx_storage/ob_ls_handle.h"

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

public:
  ObSSMCTabletInfoMap tablet_info_map_;
  int get_max_alloc_cnt(int64_t &alloc_cnt);
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

// try to alloc meta until reach mem limit, return the final alloc count
int TestSSMicroMetaManager::get_max_alloc_cnt(int64_t &alloc_cnt) {
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  bool can_alloc = true;

  ObArray<ObSSMicroBlockMetaHandle> micro_meta_handles; // record all alloced meta, free all meta after loop
  while (can_alloc) {
    void *ptr = SSMicroMetaAlloc.alloc();
    if (OB_ISNULL(ptr)) {
      can_alloc = false;
    } else {
      ObSSMicroBlockMeta *micro_meta = new(ptr) ObSSMicroBlockMeta();
      micro_meta->reset();
      ObSSMicroBlockMetaHandle micro_meta_handle;
      micro_meta_handle.set_ptr(micro_meta);
      const int64_t micro_meta_usage = SSMicroMetaAlloc.hold();
      cache_stat.mem_stat().update_micro_mem_usage(1, micro_meta_usage);
      alloc_cnt++;

      if (OB_FAIL(micro_meta_handles.push_back(micro_meta_handle))) {
        can_alloc = false;
        LOG_WARN("fail to push back micro_meta_handle", KR(ret));
      }
    }
  }
  for (int64_t i = 0; i < micro_meta_handles.count(); ++i) {
    micro_meta_handles.at(i).get_ptr()->try_free();
    micro_meta_handles.at(i).reset();
  }
  return ret;
};

// test memory cost of meta
TEST_F(TestSSMicroMetaManager, test_meta_memory_cost)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test test_meta_memory_cost");
  ObTenantBase *tenant_base = MTL_CTX();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;

  const int64_t origin_meta_cnt = micro_meta_mgr.micro_meta_map_.count();
  ASSERT_EQ(0, origin_meta_cnt);
  const int64_t origin_mem_map_cost = micro_cache->cache_stat_.mem_stat_.micro_map_usage_;
  const int64_t origin_mem_meta_cost = micro_cache->cache_stat_.mem_stat_.micro_meta_usage_;
  const int64_t origin_mem_range_cost = micro_cache->cache_stat_.mem_stat_.micro_range_usage_;
  const int64_t origin_mem_blk_cost = micro_cache->cache_stat_.mem_stat_.mem_blk_usage_;
  const int64_t origin_phy_blk_cost = micro_cache->cache_stat_.mem_stat_.phy_blk_usage_;
  const int64_t origin_mem_cost = micro_cache->cache_stat_.mem_stat_.get_total_mem_usage();
  const int64_t origin_fg_blk_used = micro_cache->cache_stat_.mem_blk_stat_.mem_blk_fg_used_cnt_;
  const int64_t origin_bg_blk_used = micro_cache->cache_stat_.mem_blk_stat_.mem_blk_bg_used_cnt_;
  LOG_INFO("init mem usage", K(origin_mem_cost), K(origin_mem_map_cost), K(origin_mem_range_cost), K(origin_mem_meta_cost),
           K(origin_mem_blk_cost), K(origin_phy_blk_cost));

  // Generate 100,000 micro and insert into mem
  const int64_t block_size = phy_blk_mgr.block_size_;
  const int64_t macro_cnt = 250;
  const int64_t micro_cnt = 400;
  const int64_t total_micro_cnt = macro_cnt * micro_cnt;
  const int32_t micro_size = 4 << 10;
  ObHashMap<ObSSMicroBlockCacheKey, int64_t> micro_key_map;
  ASSERT_EQ(OB_SUCCESS, micro_key_map.create(1 << 11,  ObMemAttr(MTL_ID(), "test")));

  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int64_t offset = 1 + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      if (OB_SUCC(micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                  macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
        struct UpdateOp {
              void operator()(HashMapPair<ObSSMicroBlockCacheKey, int64_t> &pair) { pair.second++; }
        };
        UpdateOp update_op;
        if (OB_FAIL(micro_key_map.set_or_update(micro_key, 1, update_op))) {
          LOG_WARN("fail to set_or_update micro_key", K(micro_key));
        }
      }
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }

  // Check result
  ASSERT_EQ(micro_key_map.size(), total_micro_cnt);
  const int64_t total_cnt_in_meta_map = micro_meta_mgr.micro_meta_map_.count();
  ASSERT_EQ(total_cnt_in_meta_map, total_micro_cnt);
  for (auto iter = micro_key_map.begin(); iter != micro_key_map.end(); ++iter) {
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(iter->first, micro_meta_handle, ObTabletID::INVALID_TABLET_ID, false));
  }

  const int64_t curr_mem_map_cost = micro_cache->cache_stat_.mem_stat_.micro_map_usage_;
  const int64_t curr_mem_meta_cost = micro_cache->cache_stat_.mem_stat_.micro_meta_usage_;
  const int64_t curr_mem_range_cost = micro_cache->cache_stat_.mem_stat_.micro_range_usage_;
  const int64_t curr_mem_blk_cost = micro_cache->cache_stat_.mem_stat_.mem_blk_usage_;
  const int64_t curr_phy_blk_cost = micro_cache->cache_stat_.mem_stat_.phy_blk_usage_;
  const int64_t curr_mem_cost = micro_cache->cache_stat_.mem_stat_.get_total_mem_usage();
  const int64_t curr_fg_blk_used = micro_cache->cache_stat_.mem_blk_stat_.mem_blk_fg_used_cnt_;
  const int64_t curr_bg_blk_used = micro_cache->cache_stat_.mem_blk_stat_.mem_blk_bg_used_cnt_;
  LOG_INFO("curr mem usage", K(curr_mem_cost), K(curr_mem_map_cost), K(curr_mem_range_cost), K(curr_mem_meta_cost),
           K(curr_mem_blk_cost), K(curr_phy_blk_cost));
  LOG_INFO("mem_blk usage", K(origin_fg_blk_used), K(origin_bg_blk_used), K(curr_fg_blk_used), K(curr_bg_blk_used));

  const int64_t mem_map_cost_per_meta = (curr_mem_map_cost - origin_mem_map_cost) / total_micro_cnt;
  const int64_t mem_meta_cost_per_meta = (curr_mem_meta_cost - origin_mem_meta_cost) / total_micro_cnt;
  const int64_t mem_range_cost_per_meta = (curr_mem_range_cost - origin_mem_range_cost) / total_micro_cnt;
  const int64_t mem_cost_per_micro = (curr_mem_cost - origin_mem_cost) / total_micro_cnt;
  LOG_INFO("mem usage per meta", K(mem_cost_per_micro), K(mem_map_cost_per_meta), K(mem_range_cost_per_meta), K(mem_meta_cost_per_meta));

  const int64_t meta_key_point_size = sizeof(ObSSMicroBlockCacheKey*);
  const int64_t meta_size = sizeof(ObSSMicroBlockMeta);
  const int64_t meta_handle_size = sizeof(ObSSMicroBlockMetaHandle);
  LOG_INFO("meta size", K(meta_size), K(meta_key_point_size), K(meta_handle_size));

  LOG_INFO("TEST_CASE: finsih test test_meta_memory_cost");
}

TEST_F(TestSSMicroMetaManager, basic_add_micro_meta)
{
  LOG_INFO("TEST_CASE: start basic_add_micro_meta");
  ObSSMicroMetaManager &micro_meta_mgr =  MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSMemBlockManager &mem_blk_mgr = MTL(ObSSMicroCache *)->mem_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache *)->cache_stat_;
  ObSSMemBlockPool &mem_blk_pool = mem_blk_mgr.mem_blk_pool_;
  ObSSMemBlock *mem_blk_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk_ptr, true/*is_fg*/));
  ASSERT_NE(nullptr, mem_blk_ptr);

  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = micro_meta_mgr.micro_meta_map_;
  const int64_t macro_cnt = 100;
  const int64_t micro_cnt = 100;
  const int64_t micro_size = 1024;
  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int64_t offset = 1 + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      uint32_t micro_crc = 0;
      // 1. mock micro_block add_into mem_block
      ObSSMemBlock::ObSSMemMicroInfo mem_info(j + 1, micro_size);
      ASSERT_EQ(OB_SUCCESS, mem_blk_ptr->micro_loc_map_.set_refactored(micro_key, mem_info));
      ObSSMemBlockHandle mem_blk_handle;
      mem_blk_handle.set_ptr(mem_blk_ptr);

      // 2. add_or_update micro_meta
      {
        bool real_add = false;
        const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
        ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size,
                  effective_tablet_id, micro_crc, mem_blk_handle, real_add));
        ASSERT_EQ(true, real_add);
        ObSSMicroBlockMetaHandle tmp_micro_handle;
        ASSERT_EQ(OB_SUCCESS, micro_map.get(&micro_key, tmp_micro_handle));
        ASSERT_EQ(true, tmp_micro_handle.is_valid());
        ASSERT_EQ(false, tmp_micro_handle.get_ptr()->is_data_persisted_);
        ASSERT_EQ(true, tmp_micro_handle.get_ptr()->is_in_l1_);
        ASSERT_EQ(false, tmp_micro_handle.get_ptr()->is_in_ghost_);
        ASSERT_EQ(micro_size, tmp_micro_handle.get_ptr()->size_);
      }

      // 3. add_or_update the same micro_key
      {
        bool real_add = false;
        const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
        ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size,
                  effective_tablet_id, micro_crc, mem_blk_handle, real_add));
        ASSERT_EQ(false, real_add);
        ObSSMicroBlockMetaHandle tmp_micro_handle;
        ASSERT_EQ(OB_SUCCESS, micro_map.get(&micro_key, tmp_micro_handle));
        ASSERT_EQ(true, tmp_micro_handle.is_valid());
        ASSERT_EQ(false, tmp_micro_handle.get_ptr()->is_in_l1_);
        ASSERT_EQ(false, tmp_micro_handle.get_ptr()->is_in_ghost_);
      }
    }
  }
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
}

TEST_F(TestSSMicroMetaManager, delete_and_readd_micro_meta)
{
  LOG_INFO("TEST_CASE: start delete_and_readd_micro_meta");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;

  MacroBlockId evict_macro_id;
  ObSSMicroBlockCacheKey evict_micro_key;
  const int64_t macro_cnt = 10;
  const int64_t micro_cnt = 240;
  const int64_t micro_size = 8 * 1024;

  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  // 1. add some micro_metas
  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int64_t offset = 1 + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      if (0 == i && 0 == j) {
        evict_macro_id = macro_id;
        evict_micro_key = micro_key;
      }
      ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);

  // 2. get certain micro_meta info
  ObSSMicroBlockMetaInfo cold_micro_info;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_meta_info(evict_micro_key, cold_micro_info));
  ASSERT_EQ(true, cold_micro_info.is_data_persisted_);

  // 3. try evict micro_meta
  ASSERT_EQ(false, cache_stat.task_stat().exist_arc_evict());
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_evict_micro_block_meta(cold_micro_info));
  ASSERT_EQ(true, cache_stat.task_stat().exist_arc_evict());
  ASSERT_EQ(macro_cnt * micro_cnt - 1, cache_stat.micro_stat().valid_micro_cnt_);

  // 4. try delete ghost micro_meta
  ASSERT_EQ(0, cache_stat.micro_stat().mark_del_micro_cnt_);
  ASSERT_EQ(0, cache_stat.micro_stat().mark_del_ghost_micro_cnt_);
  cold_micro_info.reset();
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_meta_info(evict_micro_key, cold_micro_info));
  ASSERT_EQ(true, cold_micro_info.is_in_ghost_);
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_delete_micro_block_meta(cold_micro_info));
  ASSERT_EQ(1, cache_stat.micro_stat().mark_del_micro_cnt_);
  ASSERT_EQ(1, cache_stat.micro_stat().mark_del_ghost_micro_cnt_);
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(macro_cnt * micro_cnt - 1, cache_stat.micro_stat().valid_micro_cnt_);

  // 5. readd deleted micro_meta
  ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(evict_micro_key, data_buf, micro_size,
            evict_macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().valid_micro_cnt_);
  ASSERT_EQ(0, cache_stat.micro_stat().mark_del_micro_cnt_);
  ASSERT_EQ(0, cache_stat.micro_stat().mark_del_ghost_micro_cnt_);
}

TEST_F(TestSSMicroMetaManager, scan_and_operate_micro_meta)
{
  LOG_INFO("TEST_CASE: start scan_and_operate_micro_meta");
  const uint64_t tenant_id = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, tablet_info_map_.create(128, ObMemAttr(tenant_id, "SSTabletInfoMap")));

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroRangeManager &micro_range_mgr = micro_cache->micro_range_mgr_;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache->mem_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMemBlockPool &mem_blk_pool = mem_blk_mgr.mem_blk_pool_;
  ObSSMemBlock *mem_blk_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk_ptr, true/*is_fg*/));
  ASSERT_NE(nullptr, mem_blk_ptr);
  const int64_t init_range_cnt = micro_range_mgr.init_range_cnt_;

  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = micro_meta_mgr.micro_meta_map_;
  const int64_t macro_cnt = 40;
  const int64_t micro_cnt = 100;
  const int64_t micro_size = 1024;
  const int32_t block_size = 2 * 1024 * 1024L;
  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int64_t offset = 1 + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      uint32_t micro_crc = 0;
      // 1. mock micro_block add_into mem_block
      ObSSMemBlock::ObSSMemMicroInfo mem_info(j + 1, micro_size);
      ASSERT_EQ(OB_SUCCESS, mem_blk_ptr->micro_loc_map_.set_refactored(micro_key, mem_info));
      ObSSMemBlockHandle mem_blk_handle;
      mem_blk_handle.set_ptr(mem_blk_ptr);
      // 2. add_or_update micro_meta
      {
        bool real_add = false;
        const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
        ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size,
                  effective_tablet_id, micro_crc, mem_blk_handle, real_add));
        ASSERT_EQ(true, real_add);
      }
      // 3. mock half micro_blocks are persisted
      if (i >= macro_cnt / 2) {
        ObSSMicroBlockMetaHandle tmp_micro_handle;
        ASSERT_EQ(OB_SUCCESS, micro_map.get(&micro_key, tmp_micro_handle));
        ASSERT_EQ(true, tmp_micro_handle.is_valid());
        tmp_micro_handle()->data_loc_ = ((i + 2) * block_size + j + 1);
        tmp_micro_handle()->is_data_persisted_ = true;
        tmp_micro_handle()->reuse_version_ = 1;
      }
    }
  }
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ObSEArray<ObLSHandle, 16> ls_handles;
  ObSSMicroCacheUtil::get_ls_handles(ls_handles);

  // scan all micro_meta
  int64_t start_range_idx1 = 0;
  int64_t end_range_idx1 = init_range_cnt - 1;
  int64_t handled_range_idx1 = -1;
  bool is_empty_range = true;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.scan_ranges_micro_block_meta(tablet_info_map_, ls_handles, start_range_idx1,
      end_range_idx1, handled_range_idx1, is_empty_range));
  ASSERT_NE(-1, handled_range_idx1);
  int64_t start_range_idx2 = handled_range_idx1 + 1;
  int64_t end_range_idx2 = init_range_cnt - 1;
  int64_t handled_range_idx2 = -1;
  is_empty_range = true;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.scan_ranges_micro_block_meta(tablet_info_map_, ls_handles, start_range_idx2,
      end_range_idx2, handled_range_idx2, is_empty_range));
  ASSERT_LT(handled_range_idx1, handled_range_idx2);
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().total_micro_cnt_);

  // mock micro_meta 'deleted'/'expired'/'abnormal'
  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    ObSSMicroBlockCacheKey deleted_micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 1, micro_size);
    ObSSMicroBlockCacheKey expired_micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 1 + micro_size, micro_size);
    ObSSMicroBlockCacheKey abnormal_micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 1 + 2 * micro_size, micro_size);

    if (i >= macro_cnt / 2) { // data persisted
      if (i == macro_cnt - 1) {
        ObSSMicroBlockMetaHandle tmp_micro_handle;
        ASSERT_EQ(OB_SUCCESS, micro_map.get(&deleted_micro_key, tmp_micro_handle));
        ASSERT_EQ(true, tmp_micro_handle.is_valid());
        ObSSMicroBlockMetaInfo deleted_micro_info;
        tmp_micro_handle()->get_micro_meta_info(deleted_micro_info);
        const int64_t ori_deleted_cnt = cache_stat.micro_stat().mark_del_micro_cnt_;
        ASSERT_EQ(OB_INVALID_ARGUMENT, micro_meta_mgr.try_delete_micro_block_meta(deleted_micro_info));
        ASSERT_EQ(ori_deleted_cnt, cache_stat.micro_stat().mark_del_micro_cnt_);
        tmp_micro_handle()->is_in_ghost_ = true;
        tmp_micro_handle()->get_micro_meta_info(deleted_micro_info);
        ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_delete_micro_block_meta(deleted_micro_info));
        ASSERT_EQ(true, tmp_micro_handle()->is_meta_deleted());
        ASSERT_EQ(ori_deleted_cnt + 1, cache_stat.micro_stat().mark_del_micro_cnt_);
        ASSERT_EQ(ori_deleted_cnt + 1, cache_stat.micro_stat().mark_del_ghost_micro_cnt_);
      } else {
        ObSSMicroBlockMetaHandle tmp_micro_handle;
        ASSERT_EQ(OB_SUCCESS, micro_map.get(&expired_micro_key, tmp_micro_handle));
        ASSERT_EQ(true, tmp_micro_handle.is_valid());
        tmp_micro_handle()->access_time_s_ = 1; // expired=true, data_persisted=true
      }
    } else { // data not persisted
      {
        ObSSMicroBlockMetaHandle tmp_micro_handle;
        ASSERT_EQ(OB_SUCCESS, micro_map.get(&expired_micro_key, tmp_micro_handle));
        ASSERT_EQ(true, tmp_micro_handle.is_valid());
        tmp_micro_handle()->access_time_s_ = 1; // expired=true, data_persisted=false
      }

      {
        ObSSMicroBlockMetaHandle tmp_micro_handle;
        ASSERT_EQ(OB_SUCCESS, micro_map.get(&abnormal_micro_key, tmp_micro_handle));
        ASSERT_EQ(true, tmp_micro_handle.is_valid());
        tmp_micro_handle()->mark_invalid(); // valid_field=false, data_persisted=false
      }
    }
  }
  ASSERT_EQ(0, cache_stat.task_stat().expired_cnt_);

  int ret = OB_SUCCESS;
  int64_t start_range_idx3 = 0;
  int64_t end_range_idx3 = init_range_cnt - 1;
  int64_t handled_range_idx3 = -1;
  do {
    is_empty_range = true;
    if (OB_FAIL(micro_meta_mgr.scan_ranges_micro_block_meta(tablet_info_map_, ls_handles, start_range_idx3,
        end_range_idx3, handled_range_idx3, is_empty_range))) {
      LOG_WARN("fail to scan ranges micro_block meta", KR(ret), K(start_range_idx3));
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    start_range_idx3 = MAX(start_range_idx3, handled_range_idx3) + 1;
    handled_range_idx3 = -1;
  } while (start_range_idx3 < init_range_cnt);

  // erase invalid_micro count = macro_cnt / 2;
  // erase expired_micro count = macro_cnt - 1;
  // erase deleted_micro count = 1
  ASSERT_EQ(macro_cnt * micro_cnt - macro_cnt - macro_cnt / 2, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(macro_cnt - 1, cache_stat.task_stat().expired_cnt_);
  ASSERT_EQ(0, cache_stat.micro_stat().mark_del_micro_cnt_);
}

TEST_F(TestSSMicroMetaManager, clear_all_micro_meta)
{
  LOG_INFO("TEST_CASE: start clear_all_micro_meta");
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  const int64_t micro_cnt = 5000;
  const int64_t micro_size = 16 * 1024;
  const int32_t block_size = 2 * 1024 * 1024L;

  ObArenaAllocator allocator;
  char *micro_data_buf = nullptr;
  if (OB_ISNULL(micro_data_buf = static_cast<char *>(allocator.alloc(micro_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(micro_size));
  } else {
    MEMSET(micro_data_buf, 'a', micro_size);
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100);
    for (int64_t j = 0; OB_SUCC(ret) && j < micro_cnt; ++j) {
      const int32_t offset = 100 + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      if (OB_FAIL(micro_cache->add_micro_block_cache(micro_key, micro_data_buf, micro_size,
                  macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
        LOG_WARN("fail to add micro_block", KR(ret), K(micro_key), KP(micro_data_buf), K(micro_size));
      } else if (j < micro_cnt / 2) {
        ObSSMicroBlockMetaHandle micro_meta_handle;
        const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
        if (OB_FAIL(micro_meta_mgr.get_micro_block_meta(micro_key, micro_meta_handle, effective_tablet_id, true/*update_arc*/))) {
          LOG_WARN("fail to get micro_block meta", KR(ret), K(micro_key));
        } else {
          ASSERT_EQ(true, micro_meta_handle.is_valid());
        }
      }

      if (j > 0 && 0 == j % 200) {
        ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
      }
    }
  }
  TestSSCommonUtil::stop_all_bg_task(micro_cache);
  ob_usleep(5_s);
  ASSERT_EQ(micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(micro_cnt / 2, arc_info.seg_info_arr_[SS_ARC_T1].count());
  ASSERT_EQ(micro_cnt / 2, arc_info.seg_info_arr_[SS_ARC_T2].count());

  micro_meta_mgr.clear();
  cache_stat.micro_stat().reset();
  ASSERT_EQ(0, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(0, cache_stat.micro_stat().valid_micro_cnt_);
  ASSERT_EQ(0, cache_stat.micro_stat().valid_micro_size_);
  ASSERT_EQ(0, cache_stat.micro_stat().mark_del_micro_cnt_);
  ASSERT_EQ(0, arc_info.seg_info_arr_[SS_ARC_T1].count());
  ASSERT_EQ(0, arc_info.seg_info_arr_[SS_ARC_T1].size());
  ASSERT_EQ(0, arc_info.seg_info_arr_[SS_ARC_T2].count());
  ASSERT_EQ(0, arc_info.seg_info_arr_[SS_ARC_T2].size());
}

TEST_F(TestSSMicroMetaManager, add_micro_meta_and_reach_mem_limit)
{
  LOG_INFO("TEST_CASE: start add_micro_meta_and_reach_mem_limit");
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSMemBlockManager &mem_blk_mgr = MTL(ObSSMicroCache *)->mem_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = MTL(ObSSMicroCache *)->cache_stat_;
  ObArenaAllocator allocator;
  char *write_buf = nullptr;

  const int64_t micro_cnt = 20;
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();
  const int64_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int64_t micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
  write_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, write_buf);
  MEMSET(write_buf, 'a', micro_size);

  MacroBlockId macro_id(0, 10000, 0);

  // 1. add micro_meta
  {
    ObSSMemBlockHandle mem_blk_handle;
    for (int64_t j = 0; j < micro_cnt; ++j) {
      bool alloc_new = false;
      uint32_t micro_crc = 0;
      int32_t offset = 1 + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.add_micro_block_data(micro_key, write_buf, micro_size, mem_blk_handle, micro_crc, alloc_new));
      ASSERT_NE(0, micro_crc);
      ASSERT_EQ(true, mem_blk_handle.is_valid());

      bool real_add = false;
      const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
      if (j == micro_cnt / 2) { // mock memory usage reach limit
        const int64_t ori_micro_cnt = cache_stat.micro_stat().total_micro_cnt_;
        cache_stat.micro_stat().total_micro_cnt_ = micro_meta_mgr.micro_cnt_limit_;
        ASSERT_EQ(OB_SS_CACHE_REACH_MEM_LIMIT,
            micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc,
            effective_tablet_id, mem_blk_handle, real_add));
        ASSERT_EQ(false, real_add);
        cache_stat.micro_stat().total_micro_cnt_ = ori_micro_cnt;
        micro_meta_mgr.set_mem_limited(false);
      } else {
        ASSERT_EQ(OB_SUCCESS,
            micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc,
            effective_tablet_id, mem_blk_handle, real_add));
        ASSERT_EQ(true, real_add);
      }
    }
  }

  // 2. check if micro_meta exist in meta_map
  {
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
      }
    }
  }
}

TEST_F(TestSSMicroMetaManager, parallel_add_micro_meta)
{
  LOG_INFO("TEST_CASE: start parallel_add_micro_meta");
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSMemBlockManager &mem_blk_mgr = MTL(ObSSMicroCache *)->mem_blk_mgr_;
  ObSSMemBlockPool &mem_blk_pool = mem_blk_mgr.mem_blk_pool_;
  ObSSMemBlock *mem_blk = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk, true/*is_fg*/));
  ASSERT_NE(nullptr, mem_blk);

  {
    int64_t fail_cnt = 0;
    ObTenantBase *tenant_base = MTL_CTX();

    auto test_func = [&](const int64_t idx) {
      ObTenantEnv::set_tenant(tenant_base);
      int ret = OB_SUCCESS;
      const int32_t micro_size = 100;
      char micro_data[micro_size];
      MEMSET(micro_data, 'a', micro_size);
      MacroBlockId macro_id;
      for (uint64_t i = 1; OB_SUCC(ret) && i <= 1000; i++) {
        if (i < 10) { // test add the same micro_block concurrently
          macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
        } else {
          macro_id = TestSSCommonUtil::gen_macro_block_id(idx * 10000 + i);
        }
        int32_t offset = idx + 1;
        ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        uint32_t micro_crc = 0;
        bool alloc_new = false;
        ObSSMemBlockHandle mem_blk_handle;
        bool real_add = false;
        const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
        if (OB_FAIL(mem_blk_mgr.add_micro_block_data(micro_key, micro_data, micro_size, mem_blk_handle, micro_crc, alloc_new))) {
          LOG_WARN("fail to add micro block", KR(ret), K(micro_key), K(micro_size));
        } else if (OB_FAIL(micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size,
                           effective_tablet_id, micro_crc, mem_blk_handle, real_add))) {
          LOG_WARN("fail to add or update micro_block meta", KR(ret), K(micro_key), K(micro_size));
        }
      }
      if (OB_FAIL(ret)) {
        ATOMIC_INC(&fail_cnt);
      }
    };

    const int64_t thread_num = 10;
    std::vector<std::thread> ths;
    for (int64_t i = 0; i < thread_num; ++i) {
      std::thread th(test_func, i);
      ths.push_back(std::move(th));
    }
    for (int64_t i = 0; i < thread_num; ++i) {
      ths[i].join();
    }
    ASSERT_EQ(0, fail_cnt);
  }
}

TEST_F(TestSSMicroMetaManager, test_scan_ranges_block_meta)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_scan_ranges_block_meta");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroRangeManager &micro_range_mgr = micro_cache->micro_range_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  const int64_t init_range_cnt = micro_range_mgr.init_range_cnt_;

  ObSEArray<ObLSHandle, 16> ls_handles;
  ObSSMicroCacheUtil::get_ls_handles(ls_handles);

  ObSSMCTabletInfoMap tablet_cache_map;
  int64_t start_range_idx = 0;
  int64_t end_range_idx = - 1;
  int64_t handled_range_idx = -1;
  tablet_cache_map.clear();
  bool is_empty_range = true;
  ASSERT_EQ(OB_INVALID_ARGUMENT, micro_meta_mgr.scan_ranges_micro_block_meta(tablet_cache_map, ls_handles, start_range_idx,
      end_range_idx, handled_range_idx, is_empty_range));
  ASSERT_EQ(-1, handled_range_idx);
  end_range_idx = init_range_cnt - 1;
  handled_range_idx = -1;
  tablet_cache_map.clear();
  is_empty_range = true;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.scan_ranges_micro_block_meta(tablet_cache_map, ls_handles, start_range_idx,
      end_range_idx, handled_range_idx, is_empty_range));
  ASSERT_EQ(end_range_idx, handled_range_idx);

  // add micro_meta
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = micro_meta_mgr.micro_meta_map_;
  const int64_t macro_cnt = 20;
  const int64_t micro_cnt = 100;
  const int64_t micro_size = 16 * 1024;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);
  const int64_t payload_offset = sizeof(ObSSPhyBlockCommonHeader) + sizeof(ObSSMicroDataBlockHeader);

  int64_t bucket_num = ObSSPersistMicroMetaOp::DEFAULT_BUCKET_NUM;
  ASSERT_EQ(OB_SUCCESS, tablet_cache_map.create(bucket_num, ObMemAttr(name::tenant_id, "SSTabletInfoMap")));
  ASSERT_EQ(0, tablet_cache_map.size());
  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int64_t offset = j * micro_size + payload_offset;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ret = micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, macro_id.second_id(), ObSSMicroCacheAccessType::COMMON_IO_TYPE);
      ASSERT_EQ(OB_SUCCESS, ret);
      if(j % 5 == 0){
        ObSSMicroBlockMetaHandle micro_handle;
        micro_meta_mgr.get_micro_block_meta(micro_key, micro_handle, macro_id.second_id(), false);
        micro_handle.get_ptr()->mark_meta_expired();
      }
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_LT(1, cache_stat.range_stat().init_range_cnt_);

  start_range_idx = 0;
  end_range_idx = init_range_cnt - 1;
  handled_range_idx = -1;
  tablet_cache_map.clear();
  do {
    is_empty_range = true;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.scan_ranges_micro_block_meta(tablet_cache_map, ls_handles, start_range_idx,
        end_range_idx, handled_range_idx, is_empty_range));
    ASSERT_GE(handled_range_idx, start_range_idx);
    start_range_idx = MAX(handled_range_idx, start_range_idx) + 1;
    handled_range_idx = -1;
  } while (start_range_idx < cache_stat.range_stat().init_range_cnt_);
  ASSERT_EQ(macro_cnt * micro_cnt - macro_cnt * micro_cnt / 5 , cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(macro_cnt, tablet_cache_map.size());
}

TEST_F(TestSSMicroMetaManager, test_micro_meta_manager_scan)
{
  LOG_INFO("TEST_CASE: start test_micro_meta_manager_scan");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroRangeManager &micro_range_mgr = micro_cache->micro_range_mgr_;
  const int64_t init_range_cnt = micro_range_mgr.init_range_cnt_;

  ObSEArray<ObLSHandle, 16> ls_handles;
  ObSSMicroCacheUtil::get_ls_handles(ls_handles);

  int64_t macro_cnt = 30;
  int64_t micro_cnt = 100;
  const int64_t micro_size = 16 * 1024;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);
  const int64_t payload_offset = sizeof(ObSSPhyBlockCommonHeader) + sizeof(ObSSMicroDataBlockHeader);

  for(int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    for(int64_t j = 0; j < micro_cnt; ++j) {
      const int64_t offset = j * micro_size + payload_offset;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, macro_id.second_id(), ObSSMicroCacheAccessType::COMMON_IO_TYPE);
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(micro_key, micro_meta_handle, macro_id.second_id(), false));
      if(j % 10 == 0) {
        micro_meta_handle.get_ptr()->mark_deleted();
      }
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(macro_cnt * micro_cnt, cache_stat.micro_stat().total_micro_cnt_);

  ObSSMCTabletInfoMap tablet_cache_map;
  int64_t bucket_num = ObSSPersistMicroMetaOp::DEFAULT_BUCKET_NUM;
  ASSERT_EQ(OB_SUCCESS, tablet_cache_map.create(bucket_num, ObMemAttr(name::tenant_id, "SSTabletInfoMap")));
  ASSERT_EQ(0, tablet_cache_map.size());

  int64_t start_range_idx = 0;
  int64_t end_range_idx = init_range_cnt - 1;
  int64_t handled_range_idx = -1;
  bool is_empty_range = true;
  do {
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.scan_ranges_micro_block_meta(tablet_cache_map, ls_handles, start_range_idx, end_range_idx,
        handled_range_idx, is_empty_range));
    ASSERT_GE(handled_range_idx, start_range_idx);
    start_range_idx = MAX(start_range_idx, handled_range_idx) + 1;
    handled_range_idx = -1;
    is_empty_range = true;
  } while(start_range_idx < init_range_cnt);
  ASSERT_EQ(macro_cnt * micro_cnt - macro_cnt * micro_cnt / 10, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(macro_cnt, tablet_cache_map.size());

  for(int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100 + i);
    for(int64_t j = 0; j < micro_cnt; j+=10) {
      const int64_t offset = j * micro_size + payload_offset;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.get_micro_block_meta(micro_key, micro_meta_handle, macro_id.second_id(), false));
    }
  }

  int64_t total_meta_cnt = 0;
  ObSSMicroInitRangeInfo **init_range_arr = micro_range_mgr.get_init_range_arr();
  for(int64_t i = 0; i < cache_stat.range_stat().init_range_cnt_; ++i) {
    // calculate the sum of meta range in each init range
    if(init_range_arr[i]->has_sub_ranges()) {
      ObSSMicroSubRangeInfo *cur_sub_range = init_range_arr[i]->get_first_sub_range();
      while(nullptr != cur_sub_range){
        total_meta_cnt += cur_sub_range->get_micro_meta_cnt();
        cur_sub_range = cur_sub_range->next_;
      }
    }
  }
  ASSERT_EQ(macro_cnt * micro_cnt - macro_cnt * micro_cnt / 10, total_meta_cnt);
}

TEST_F(TestSSMicroMetaManager, test_persist_micro_parallel_with_clear)
{
  LOG_INFO("TEST: start test_persist_micro_parallel_with_clear");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const int64_t block_size = micro_cache->phy_blk_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSPhysicalBlockManager *phy_blk_mgr = &micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager *micro_meta_mgr = &micro_cache->get_micro_meta_mgr();

  const int64_t WRITE_BLK_CNT = 50;
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 100;
  const int32_t micro_size = (block_size - payload_offset) / micro_cnt - micro_index_size;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  ASSERT_NE(true, micro_cache->task_runner_.is_task_closed());

  for (int64_t i = 0; i < WRITE_BLK_CNT; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                            macro_id.second_id()/*effective_tablet_id*/, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->get_micro_block_meta(micro_key, micro_meta_handle, macro_id.second_id(), false));
      if (j % 10 == 0) {
        micro_meta_handle.get_ptr()->mark_deleted();
      }
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }

  ObTenantBase *tenant_base = MTL_CTX();
  std::thread clear_thread([&]( ) {
    ob_usleep(78 * 1000L);
    ObTenantEnv::set_tenant(tenant_base);
    IGNORE_RETURN micro_cache->clear_micro_cache();
  });

  // persist micro meta
  ObSSPersistMicroMetaTask* persist_meta_task = &micro_cache->task_runner_.persist_meta_task_;
  persist_meta_task->cur_interval_us_ = 3600 * 1000 * 1000L;
  ob_usleep(2 * 1000 * 1000L);

  persist_meta_task->is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task->persist_meta_op_.start_op());
  persist_meta_task->persist_meta_op_.micro_ckpt_ctx_.need_ckpt_ = true;
  ASSERT_EQ(OB_SUCCESS, persist_meta_task->persist_meta_op_.gen_checkpoint());
  persist_meta_task->persist_meta_op_.is_closed_ = true;

  clear_thread.join();
}

TEST_F(TestSSMicroMetaManager, test_micro_cnt_limit)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST: start test_micro_cnt_limit");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const int64_t block_size = micro_cache->phy_blk_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSPhysicalBlockManager *phy_blk_mgr = &micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager *micro_meta_mgr = &micro_cache->get_micro_meta_mgr();
  const int64_t micro_cnt_limit = micro_meta_mgr->micro_cnt_limit_;
  ASSERT_LT(0, micro_cnt_limit);
  LOG_INFO("check micro_cnt_limit", K(micro_cnt_limit), K(micro_meta_mgr->mem_limit_size_));

  ObArray<ObSSMicroBlockMetaHandle> micro_meta_handles;
  for (int64_t i = 0; i < micro_cnt_limit; ++i) {
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->alloc_micro_block_meta(micro_meta_handle));
    ASSERT_EQ(true, micro_meta_handle.is_valid());
    ASSERT_EQ(OB_SUCCESS, micro_meta_handles.push_back(micro_meta_handle));
  }
  ASSERT_EQ(micro_cnt_limit, micro_meta_handles.count());
}

TEST_F(TestSSMicroMetaManager, test_micro_alloc_limit_when_increasing_memory)
{
  LOG_INFO("TEST: start test_micro_alloc_limit_when_increasing_memory");
  ObTenantBase *tenant_base = MTL_CTX();
  ObTenantEnv::set_tenant(tenant_base);
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroMetaManager *micro_meta_mgr = &micro_cache->get_micro_meta_mgr();
  ObSSMicroCacheMemoryStat &mem_stat = cache_stat.mem_stat_;
  ObSSMicroCacheMicroStat &micro_stat = cache_stat.micro_stat_;

  // record original memory data
  const int64_t original_mtl_mem_size = MTL_MEM_SIZE();
  const int64_t original_mem_limit_size = micro_meta_mgr->mem_limit_size_;
  const int64_t original_mem_limit_cnt = mem_stat.micro_cnt_limit_;
  int64_t original_max_alloc_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, get_max_alloc_cnt(original_max_alloc_cnt));

  // memory increase 1x
  int64_t curr_mtl_mem_size = 2 * original_mtl_mem_size;
  tenant_base->set_unit_memory_size(curr_mtl_mem_size);
  sleep(3);
  // get current memory data
  ASSERT_EQ(0, mem_stat.get_micro_alloc_cnt());
  const int64_t curr_mem_limit_size = micro_meta_mgr->mem_limit_size_;
  const int64_t curr_mem_limit_cnt = micro_meta_mgr->micro_cnt_limit_;
  int64_t curr_max_alloc_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, get_max_alloc_cnt(curr_max_alloc_cnt));
  // check if alloc limit is increased, and consistent with micro_cnt_limit
  LOG_INFO("curr_alloc_cnt", K(original_mem_limit_size), K(original_mem_limit_cnt), K(original_max_alloc_cnt),
                             K(curr_mem_limit_size),K(curr_mem_limit_cnt), K(curr_max_alloc_cnt));
  ASSERT_GT(curr_max_alloc_cnt, original_max_alloc_cnt);
  ASSERT_EQ(curr_max_alloc_cnt, curr_mem_limit_cnt);

  // modify memory to original value
  tenant_base->set_unit_memory_size(original_mtl_mem_size);
  sleep(3);
}

// Test bitfield race condition: concurrent access to sub_range bitfields
// Thread 1: calls init_range->set_sub_range_offset_len() to modify offset_ and length_
// Thread 2: calls init_range->link_micro_meta_directly() to modify micro_meta_cnt_
TEST_F(TestSSMicroMetaManager, test_sub_range_bitfield_race_condition)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_sub_range_bitfield_race_condition");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroRangeManager &micro_range_mgr = micro_cache->micro_range_mgr_;
  ObSSMicroMetaManager *micro_meta_mgr = &micro_cache->get_micro_meta_mgr();

  const int64_t TEST_LOOP_CNT = 100000;
  const uint64_t EXPECTED_OFFSET = 100;
  const uint64_t EXPECTED_LENGTH = 100;
  const int64_t EXPECTED_MICRO_CNT = 2;
  int64_t error_cnt = 0;

  // Create init_range and sub_range for each iteration
  ObSSMicroInitRangeInfo *init_range = nullptr;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.create_init_range(init_range));
  ASSERT_NE(nullptr, init_range);
  init_range->end_hval_ = SS_MICRO_KEY_MAX_HASH_VAL;

  ObSSMicroSubRangeInfo *sub_range = nullptr;
  ASSERT_EQ(OB_SUCCESS, micro_range_mgr.create_sub_range(sub_range, false));
  ASSERT_NE(nullptr, sub_range);
  init_range->next_ = sub_range;

  // Pre-allocate micro_meta
  ObSSMicroBlockMetaHandle micro_meta_handle;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr->alloc_micro_block_meta(micro_meta_handle));
  ASSERT_EQ(true, micro_meta_handle.is_valid());

  // Concurrent test: in each loop iteration, two threads concurrently modify different bitfields
  for (int64_t i = 0; i < TEST_LOOP_CNT; ++i) {
    // Reset sub_range
    sub_range->reset();
    sub_range->set_end_hval(SS_MICRO_KEY_MAX_HASH_VAL);
    sub_range->offset_ = 0;
    sub_range->length_ = 0;
    sub_range->micro_meta_cnt_ = 1;

    std::thread thread1([&]() {
      ObTenantBase *tenant_base = MTL_CTX();
      ObTenantEnv::set_tenant(tenant_base);
      ASSERT_EQ(OB_SUCCESS, init_range->set_sub_range_offset_len(sub_range, EXPECTED_OFFSET, EXPECTED_LENGTH));
    });

    std::thread thread2([&]() {
      ObTenantBase *tenant_base = MTL_CTX();
      ObTenantEnv::set_tenant(tenant_base);
      ASSERT_EQ(OB_SUCCESS, init_range->link_micro_meta_directly(sub_range, micro_meta_handle));
    });

    thread1.join();
    thread2.join();

    // Check values after each concurrent operation
    const uint64_t final_offset = sub_range->offset_;
    const uint64_t final_length = sub_range->length_;
    const int64_t final_micro_cnt = sub_range->micro_meta_cnt_;

    if (final_offset != EXPECTED_OFFSET || final_length != EXPECTED_LENGTH || final_micro_cnt != EXPECTED_MICRO_CNT) {
      LOG_WARN("TEST_CASE: bitfield race condition detected",
          "loop_idx", i,
          "final_offset", final_offset,
          "final_length", final_length,
          "final_micro_cnt", final_micro_cnt,
          "expected_offset", EXPECTED_OFFSET,
          "expected_length", EXPECTED_LENGTH,
          "expected_micro_cnt", EXPECTED_MICRO_CNT);
      error_cnt++;
    }
  }

  micro_meta_handle.reset();
  micro_range_mgr.destroy_sub_range(sub_range);
  OB_DELETEx(ObSSMicroInitRangeInfo, &micro_range_mgr.allocator_, init_range);

  ASSERT_EQ(0, error_cnt);
  LOG_INFO("TEST_CASE: finish test_sub_range_bitfield_race_condition");
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_meta_manager.log*");
  OB_LOGGER.set_file_name("test_ss_micro_meta_manager.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}