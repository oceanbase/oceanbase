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

#define USING_LOG_PREFIX STORAGETEST
#include <gtest/gtest.h>

#define protected public
#define private public
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#include "lib/thread/threads.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashset.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

class TestSSPhysicalBlockManager : public ::testing::Test
{
public:
  TestSSPhysicalBlockManager() {}
  virtual ~TestSSPhysicalBlockManager() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  int serialize_super_block(char *buf, const int64_t buf_size, const ObSSMicroCacheSuperBlk &super_blk);
  int deserialize_super_block(char *buf, const int64_t buf_size, ObSSMicroCacheSuperBlk &super_blk);
  void check_super_block_identical(const ObSSMicroCacheSuperBlk &super_blk1, const ObSSMicroCacheSuperBlk &super_blk2);
};

void TestSSPhysicalBlockManager::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSPhysicalBlockManager::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSPhysicalBlockManager::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32), 1/*micro_split_cnt*/));
  micro_cache->start();
}

void TestSSPhysicalBlockManager::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}


int TestSSPhysicalBlockManager::serialize_super_block(
    char *buf,
    const int64_t buf_size,
    const ObSSMicroCacheSuperBlk &super_blk)
{
  int ret = OB_SUCCESS;
  ObSSPhyBlockCommonHeader common_header;
  int64_t pos = common_header.header_size_;
  if (OB_UNLIKELY(nullptr == buf || buf_size <= common_header.header_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(super_blk.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize super_block", KR(ret), KP(buf), K(buf_size), K(pos));
  } else {
    common_header.set_payload_size(pos - common_header.header_size_);
    common_header.set_block_type(ObSSPhyBlockType::SS_SUPER_BLK);
    common_header.calc_payload_checksum(buf + common_header.header_size_, common_header.payload_size_);
    pos = 0;
    if (OB_FAIL(common_header.serialize(buf, buf_size, pos))) {
      LOG_WARN("fail to serialize common header", KR(ret), KP(buf), K(buf_size), K(pos));
    } else if (OB_UNLIKELY(pos != common_header.header_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pos is wrong", KR(ret), K(pos), K_(common_header.header_size));
    }
  }
  return ret;
}

int TestSSPhysicalBlockManager::deserialize_super_block(
    char *buf,
    const int64_t buf_size,
    ObSSMicroCacheSuperBlk &super_blk)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroCacheSuperBlk tmp_super_blk;
  if (OB_UNLIKELY(nullptr == buf || buf_size <= common_header.header_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(common_header.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize common header", KR(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_UNLIKELY(common_header.header_size_ != pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized pos is unexpected", KR(ret), K(common_header), K(pos));
  } else if (OB_UNLIKELY(!common_header.is_valid() || (!common_header.is_super_blk()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized common header is invalid or wrong type", KR(ret), K(common_header));
  } else if (OB_FAIL(common_header.check_payload_checksum(buf + pos, common_header.payload_size_))) {
    LOG_WARN("fail to check common header payload checksum", KR(ret), K(common_header), KP(buf), K(pos));
  } else if (OB_FAIL(tmp_super_blk.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize super_block", KR(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_UNLIKELY(!tmp_super_blk.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized super_block is invalid", KR(ret), K(tmp_super_blk), K(pos));
  } else if (OB_FAIL(super_blk.assign(tmp_super_blk))) {
    LOG_WARN("fail to assign ss_super_block", KR(ret), K(tmp_super_blk));
  }
  return ret;
}

void TestSSPhysicalBlockManager::check_super_block_identical(
    const ObSSMicroCacheSuperBlk &super_blk1,
    const ObSSMicroCacheSuperBlk &super_blk2)
{
  ASSERT_EQ(super_blk1.micro_ckpt_time_us_, super_blk2.micro_ckpt_time_us_);
  ASSERT_EQ(super_blk1.cache_file_size_, super_blk2.cache_file_size_);
  ASSERT_EQ(super_blk1.modify_time_us_, super_blk2.modify_time_us_);
  ASSERT_EQ(super_blk1.micro_ckpt_info_, super_blk2.micro_ckpt_info_);
  ASSERT_EQ(super_blk1.blk_ckpt_info_, super_blk2.blk_ckpt_info_);
}

// test data_blk allocation performance
TEST_F(TestSSPhysicalBlockManager, test_data_blk_allocation_performance)
{
  LOG_INFO("TEST_CASE: start test test_phy_blk_allocation_performance");
  ObTenantBase *tenant_base = MTL_CTX();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  std::atomic<int64_t> total_cost_us = 0;
  std::atomic<int64_t> total_alloc_cnt = 0;
  int64_t reorgan_blk_cnt = phy_blk_mgr.blk_cnt_info_.reorgan_blk_cnt_;
  LOG_INFO("blk cnt before test", K(phy_blk_mgr.blk_cnt_info_), K(reorgan_blk_cnt));

  auto alloc_phy_blk = [&](const int64_t thread_index, const int64_t alloc_num, const ObSSPhyBlockType block_type,
                           std::atomic<int64_t> &total_cost_us, std::atomic<int64_t> &total_alloc_cnt) {
    ObTenantEnv::set_tenant(tenant_base);
    int64_t phy_blk_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;

    for (int i = 0; i < alloc_num; ++i) {
      const int64_t start_us = ObTimeUtility::current_time_us();
      int ret = phy_blk_mgr.alloc_block(phy_blk_idx, phy_blk_handle, block_type);
      const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
      if (OB_SUCC(ret)) {
        LOG_INFO("succ to alloc free phy_block", K(thread_index), KR(ret), K(block_type), K(phy_blk_idx), K(cost_us));
        total_cost_us.fetch_add(cost_us);
        total_alloc_cnt.fetch_add(1);
      } else if (OB_EAGAIN == ret) {
        LOG_WARN("no block to alloc free phy_block", K(thread_index), KR(ret), K(block_type));
      } else {
        LOG_WARN("some error when alloc free phy_block", K(thread_index), KR(ret), K(block_type));
      }
    }
  };
  // each thread apply to alloc max_cnt/2 data_blk
  std::vector<std::thread> th_alloc_blk;
  const int64_t thread_num = 4;
  ObSSPhyBlockType block_type = ObSSPhyBlockType::SS_MICRO_DATA_BLK;
  const int64_t data_blk_max_cnt = phy_blk_mgr.blk_cnt_info_.data_blk_.max_cnt_;
  const int64_t alloc_num = data_blk_max_cnt / 2;

  for (int64_t i = 0; i < thread_num; ++i) {
    std::thread th(alloc_phy_blk, i, alloc_num, block_type, std::ref(total_cost_us), std::ref(total_alloc_cnt));
    th_alloc_blk.push_back(std::move(th));
  }
  for (int64_t i = 0; i < thread_num; ++i) {
    th_alloc_blk[i].join();
  }

  ASSERT_NE(0, total_alloc_cnt);
  ASSERT_EQ(total_alloc_cnt + reorgan_blk_cnt, data_blk_max_cnt);
  int64_t avg_time_us = total_cost_us / total_alloc_cnt;
  LOG_INFO("blk cnt after test", K(phy_blk_mgr.blk_cnt_info_), K(reorgan_blk_cnt));
  LOG_INFO("allocation performance", K(total_alloc_cnt.load()), K(avg_time_us));
  // phy_blk alloc time should not exceed 400, otherwise it will be considered as performance regression.
  ASSERT_LT(avg_time_us, 400);

  LOG_INFO("TEST_CASE: finsih test test_phy_blk_allocation_performance");
}

// test different phy_blk allocation performance concurrently
TEST_F(TestSSPhysicalBlockManager, test_different_blk_allocation_performance)
{
  LOG_INFO("TEST_CASE: start test test_phy_blk_allocation_performance");
  ObTenantBase *tenant_base = MTL_CTX();
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  std::atomic<int64_t> total_data_blk_alloc_cnt = 0;
  std::atomic<int64_t> total_meta_blk_alloc_cnt = 0;
  std::atomic<int64_t> total_reorgan_blk_alloc_cnt = 0;
  std::atomic<int64_t> total_data_blk_cost_us = 0;
  std::atomic<int64_t> total_meta_blk_cost_us = 0;
  std::atomic<int64_t> total_reorgan_blk_cost_us = 0;
  const int64_t data_blk_max_cnt = phy_blk_mgr.blk_cnt_info_.data_blk_.max_cnt_;
  const int64_t meta_blk_max_cnt = phy_blk_mgr.blk_cnt_info_.meta_blk_.max_cnt_;
  int64_t reorgan_blk_cnt = phy_blk_mgr.blk_cnt_info_.reorgan_blk_cnt_;
  LOG_INFO("before test blk cnt", K(phy_blk_mgr.cache_stat_.phy_blk_stat_), K(reorgan_blk_cnt));

  auto alloc_phy_blk = [&](const int64_t thread_index, const int64_t alloc_num, const ObSSPhyBlockType block_type,
                           std::atomic<int64_t> &data_blk_alloc_cnt, std::atomic<int64_t> &data_blk_cost_us,
                           std::atomic<int64_t> &meta_blk_alloc_cnt, std::atomic<int64_t> &meta_blk_cost_us,
                           std::atomic<int64_t> &reorgan_blk_alloc_cnt, std::atomic<int64_t> &reorgan_blk_cost_us) {
    ObTenantEnv::set_tenant(tenant_base);
    int64_t phy_blk_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;

    for (int i = 0; i < alloc_num; ++i) {
      const int64_t start_us = ObTimeUtility::current_time_us();
      int ret = phy_blk_mgr.alloc_block(phy_blk_idx, phy_blk_handle, block_type);
      const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
      if (OB_SUCC(ret)) {
        LOG_INFO("succ to alloc free phy_block", K(thread_index), KR(ret), K(block_type), K(phy_blk_idx), K(cost_us));
        if (ObSSPhyBlockType::SS_MICRO_DATA_BLK == block_type) {
          data_blk_cost_us.fetch_add(cost_us);
          data_blk_alloc_cnt.fetch_add(1);
        } else if (ObSSPhyBlockType::SS_MICRO_META_BLK == block_type) {
          meta_blk_cost_us.fetch_add(cost_us);
          meta_blk_alloc_cnt.fetch_add(1);
        } else {
          reorgan_blk_cost_us.fetch_add(cost_us);
          reorgan_blk_alloc_cnt.fetch_add(1);
        }
      } else if (OB_EAGAIN == ret) {
        LOG_WARN("no block to alloc free phy_block", K(thread_index), KR(ret), K(block_type));
      } else {
        LOG_WARN("some error when alloc free phy_block", K(thread_index), KR(ret), K(block_type));
      }
    }
  };
  // each thread apply to alloc max_cnt/2 phy_blk, and per kind of phy_blk will be allocated by 3 thread concurrently
  std::vector<std::thread> th_alloc_blk;
  const int64_t thread_num = 9;
  int64_t alloc_num[3];
  ObSSPhyBlockType block_type[3];
  block_type[0] = ObSSPhyBlockType::SS_MICRO_DATA_BLK;
  block_type[1] = ObSSPhyBlockType::SS_MICRO_META_BLK;
  block_type[2] = ObSSPhyBlockType::SS_REORGAN_BLK;
  alloc_num[0] = data_blk_max_cnt / 2;
  alloc_num[1] = meta_blk_max_cnt / 2;
  alloc_num[2] = reorgan_blk_cnt / 2;

  LOG_INFO("shared blk info", K(phy_blk_mgr.blk_cnt_info_.shared_blk_cnt_), K(phy_blk_mgr.blk_cnt_info_.shared_blk_used_cnt_));
  for (int64_t i = 0; i < thread_num; ++i) {
    std::thread th(alloc_phy_blk, i, alloc_num[i % 3], block_type[i % 3],
                   std::ref(total_data_blk_alloc_cnt), std::ref(total_data_blk_cost_us),
                   std::ref(total_meta_blk_alloc_cnt), std::ref(total_meta_blk_cost_us),
                   std::ref(total_reorgan_blk_alloc_cnt), std::ref(total_reorgan_blk_cost_us));
    th_alloc_blk.push_back(std::move(th));
  }
  for (int64_t i = 0; i < thread_num; ++i) {
    th_alloc_blk[i].join();
  }

  const int64_t data_blk_used = phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_;
  ASSERT_EQ(total_data_blk_alloc_cnt + total_reorgan_blk_alloc_cnt, data_blk_used);
  ASSERT_LE(data_blk_used, data_blk_max_cnt);
  ASSERT_EQ(total_meta_blk_alloc_cnt, meta_blk_max_cnt);
  int64_t avg_data_blk_time_us = total_data_blk_cost_us / total_data_blk_alloc_cnt;
  int64_t avg_meta_blk_time_us = total_meta_blk_cost_us / total_meta_blk_alloc_cnt;
  int64_t avg_reorgan_blk_time_us = total_reorgan_blk_alloc_cnt / total_reorgan_blk_alloc_cnt;
  LOG_INFO("allocation performance", K(total_data_blk_alloc_cnt.load()), K(avg_data_blk_time_us),
                                     K(total_meta_blk_alloc_cnt.load()), K(avg_meta_blk_time_us),
                                     K(total_reorgan_blk_alloc_cnt.load()), K(avg_reorgan_blk_time_us));
  // phy_blk alloc time should not exceed 400, otherwise it will be considered as performance regression.
  ASSERT_LT(avg_data_blk_time_us, 400);
  ASSERT_LT(avg_meta_blk_time_us, 400);
  ASSERT_LT(avg_reorgan_blk_time_us, 400);

  LOG_INFO("TEST_CASE: finsih test test_phy_blk_allocation_performance");
}


TEST_F(TestSSPhysicalBlockManager, super_block)
{
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ASSERT_EQ(true, phy_blk_mgr.is_inited_);
  phy_blk_mgr.super_blk_.reset();
  ASSERT_EQ(false, phy_blk_mgr.super_blk_.is_valid());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.format_ss_super_block(SS_PERSIST_MICRO_CKPT_SPLIT_CNT));
  ASSERT_EQ(true, phy_blk_mgr.super_blk_.is_valid());
  ObSSMicroCacheSuperBlk super_blk;
  ASSERT_EQ(false, super_blk.is_valid());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_ss_super_block(super_blk));
  ASSERT_EQ(true, super_blk.is_valid());
  ASSERT_EQ(MTL_ID(), super_blk.tenant_id_);
  super_blk.modify_time_us_ = 1000001;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_ss_super_block(super_blk));
  ASSERT_EQ(true, phy_blk_mgr.super_blk_.is_valid());
  ASSERT_EQ(super_blk.modify_time_us_, phy_blk_mgr.super_blk_.modify_time_us_);
  ASSERT_EQ(false, super_blk.is_valid_checkpoint());
}

TEST_F(TestSSPhysicalBlockManager, basic_physical_block)
{
  LOG_INFO("TEST_CASE: start basic_physical_block");
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = phy_blk_mgr.cache_stat_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;

  // 1. Check blk_count_info
  {
    ASSERT_EQ(true, blk_cnt_info.is_valid());
    ASSERT_EQ(blk_cnt_info.total_blk_cnt_, blk_cnt_info.super_blk_cnt_ +
                                          blk_cnt_info.shared_blk_cnt_ +
                                          blk_cnt_info.phy_ckpt_blk_cnt_);
    const int64_t data_blk_min_cnt = MAX(blk_cnt_info.reorgan_blk_cnt_, blk_cnt_info.shared_blk_cnt_ * SS_MIN_MICRO_DATA_BLK_CNT_PCT / 100);
    const int64_t meta_blk_min_cnt = MAX(2, blk_cnt_info.shared_blk_cnt_ * SS_MIN_MICRO_META_BLK_CNT_PCT / 100);
    ASSERT_EQ(data_blk_min_cnt, blk_cnt_info.data_blk_.min_cnt_);
    ASSERT_EQ(data_blk_min_cnt, blk_cnt_info.data_blk_.hold_cnt_);
    ASSERT_EQ(meta_blk_min_cnt, blk_cnt_info.meta_blk_.min_cnt_);
    ASSERT_EQ(meta_blk_min_cnt, blk_cnt_info.meta_blk_.hold_cnt_);
    ASSERT_EQ(blk_cnt_info.shared_blk_cnt_, blk_cnt_info.data_blk_.min_cnt_ + blk_cnt_info.meta_blk_.max_cnt_);
    ASSERT_EQ(blk_cnt_info.shared_blk_cnt_, blk_cnt_info.data_blk_.max_cnt_ + blk_cnt_info.meta_blk_.min_cnt_);
    ASSERT_EQ(data_blk_min_cnt, cache_stat.phy_blk_stat().data_blk_cnt_);
    ASSERT_EQ(meta_blk_min_cnt, cache_stat.phy_blk_stat().meta_blk_cnt_);
  }

  // 2. Test alloc and free phy_blk
  {
    ObSSPhyBlockHandle phy_blk_handle;
    int64_t phy_blk_idx = -1;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(phy_blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
    ASSERT_LT(0, phy_blk_idx);
    ASSERT_EQ(true, phy_blk_handle.is_valid());
    ASSERT_EQ(1, blk_cnt_info.data_blk_.used_cnt_);
    ASSERT_EQ(1, cache_stat.phy_blk_stat().data_blk_used_cnt_);
    phy_blk_handle()->set_sealed(50);

    ASSERT_EQ(OB_SUCCESS, phy_blk_handle()->dec_valid_len(phy_blk_idx, -50));
    ASSERT_EQ(true, phy_blk_handle()->is_empty());
    ASSERT_EQ(1, phy_blk_mgr.get_reusable_blocks_cnt());
    ASSERT_EQ(2, phy_blk_handle()->ref_cnt_);

    bool succ_free = false;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(phy_blk_idx, succ_free));
    ASSERT_EQ(false, succ_free);
    phy_blk_handle.reset();
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(phy_blk_idx, succ_free));
    ASSERT_EQ(true, succ_free);

    // test double free phy_block
    succ_free = false;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(phy_blk_idx, succ_free));
    ASSERT_EQ(false, succ_free);
    ASSERT_EQ(0, blk_cnt_info.data_blk_.used_cnt_);
    ASSERT_EQ(0, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  }
}


TEST_F(TestSSPhysicalBlockManager, basic_alloc_and_free)
{
  LOG_INFO("TEST_CASE: start basic_alloc_and_free");
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = phy_blk_mgr.cache_stat_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;

  // 1. data_block
  {
    // allocate all avaliable data_block
    const int64_t data_blk_cnt = blk_cnt_info.micro_data_blk_max_cnt();
    ASSERT_LT(0, data_blk_cnt);
    int64_t block_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ObArray<int64_t> data_blk_idx_arr;
    for (int64_t i = 0; i < data_blk_cnt; i++) {
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
      ASSERT_EQ(OB_SUCCESS, data_blk_idx_arr.push_back(block_idx));
      ASSERT_EQ(true, phy_blk_handle.is_valid());
      phy_blk_handle.get_ptr()->is_sealed_ = true;
      phy_blk_handle.reset();
    }

    ASSERT_LT(blk_cnt_info.data_blk_.min_cnt_, blk_cnt_info.data_blk_.hold_cnt_);
    ASSERT_EQ(data_blk_cnt, blk_cnt_info.data_blk_.used_cnt_);
    ASSERT_EQ(data_blk_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
    ASSERT_EQ(blk_cnt_info.data_blk_.max_cnt_, blk_cnt_info.data_blk_.hold_cnt_);
    ASSERT_EQ(blk_cnt_info.data_blk_.max_cnt_, cache_stat.phy_blk_stat().data_blk_cnt_);

    int64_t shared_blk_used_cnt = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;
    ASSERT_EQ(shared_blk_used_cnt, blk_cnt_info.shared_blk_used_cnt_);
    ASSERT_EQ(shared_blk_used_cnt, cache_stat.phy_blk_stat().shared_blk_used_cnt_);
    {
      ASSERT_EQ(OB_EAGAIN, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
    }

    // free all data_blk
    bool succ_free = false;
    for (int64_t i = 0; i < data_blk_cnt; i++) {
      succ_free = false;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(data_blk_idx_arr.at(i)));
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(data_blk_idx_arr.at(i), succ_free));
      ASSERT_EQ(true, succ_free);
    }
    ASSERT_EQ(0, blk_cnt_info.data_blk_.used_cnt_);
    ASSERT_EQ(0, cache_stat.phy_blk_stat().data_blk_used_cnt_);
    ASSERT_EQ(blk_cnt_info.data_blk_.min_cnt_, blk_cnt_info.data_blk_.hold_cnt_);
    ASSERT_EQ(blk_cnt_info.data_blk_.min_cnt_, cache_stat.phy_blk_stat().data_blk_cnt_);
  }

  // 2. phy_blk_ckpt
  {
    // allocate all phy_blk_ckpt block
    int64_t phy_ckpt_blk_cnt = phy_blk_mgr.blk_cnt_info_.phy_ckpt_blk_cnt_;
    ASSERT_LT(0, phy_ckpt_blk_cnt);
    int64_t block_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ObArray<int64_t> phy_ckpt_blk_idx_arr;
    for (int64_t i = 0; i < phy_ckpt_blk_cnt; i++) {
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
      ASSERT_EQ(OB_SUCCESS, phy_ckpt_blk_idx_arr.push_back(block_idx));
      ASSERT_EQ(true, phy_blk_handle.is_valid());
      phy_blk_handle.get_ptr()->is_sealed_ = true;
      phy_blk_handle.reset();
    }
    ASSERT_EQ(phy_ckpt_blk_cnt, blk_cnt_info.phy_ckpt_blk_used_cnt_);
    ASSERT_EQ(phy_ckpt_blk_cnt, cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);
    {
      ASSERT_EQ(OB_EAGAIN, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
    }

    // free all phy_blk_ckpt block
    bool succ_free = false;
    for (int64_t i = 0; i < phy_ckpt_blk_cnt; i++) {
      succ_free = false;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(phy_ckpt_blk_idx_arr.at(i)));
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(phy_ckpt_blk_idx_arr.at(i), succ_free));
      ASSERT_EQ(true, succ_free);
    }
    ASSERT_EQ(0, blk_cnt_info.phy_ckpt_blk_used_cnt_);
    ASSERT_EQ(0, cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);
  }

  // 3. meta_block
  {
    // allocate all meta_block
    const int64_t micro_ckpt_blk_cnt = blk_cnt_info.meta_blk_.max_cnt_;
    ASSERT_LT(0, micro_ckpt_blk_cnt);
    int64_t block_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ObArray<int64_t> micro_ckpt_blk_idx_arr;
    for (int64_t i = 0; i < micro_ckpt_blk_cnt; i++) {
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_META_BLK));
      ASSERT_EQ(OB_SUCCESS, micro_ckpt_blk_idx_arr.push_back(block_idx));
      ASSERT_EQ(true, phy_blk_handle.is_valid());
      phy_blk_handle.get_ptr()->is_sealed_ = true;
      phy_blk_handle.reset();
    }
    ASSERT_LT(blk_cnt_info.meta_blk_.min_cnt_, blk_cnt_info.meta_blk_.hold_cnt_);
    ASSERT_EQ(blk_cnt_info.meta_blk_.max_cnt_, blk_cnt_info.meta_blk_.hold_cnt_);
    ASSERT_EQ(micro_ckpt_blk_cnt, blk_cnt_info.meta_blk_.used_cnt_);
    ASSERT_EQ(micro_ckpt_blk_cnt, phy_blk_mgr.cache_stat_.phy_blk_stat().meta_blk_used_cnt_);
    ASSERT_EQ(micro_ckpt_blk_cnt, blk_cnt_info.meta_blk_.hold_cnt_);
    ASSERT_EQ(micro_ckpt_blk_cnt, phy_blk_mgr.cache_stat_.phy_blk_stat().meta_blk_cnt_);

    int64_t shared_blk_used_cnt = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;
    ASSERT_EQ(shared_blk_used_cnt, blk_cnt_info.shared_blk_used_cnt_);
    ASSERT_EQ(shared_blk_used_cnt, cache_stat.phy_blk_stat().shared_blk_used_cnt_);
    {
      ASSERT_EQ(OB_EAGAIN, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_META_BLK));
    }

    // free all meta_block
    bool succ_free = false;
    for (int64_t i = 0; i < micro_ckpt_blk_cnt; i++) {
      succ_free = false;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(micro_ckpt_blk_idx_arr.at(i)));
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(micro_ckpt_blk_idx_arr.at(i), succ_free));
      ASSERT_EQ(true, succ_free);
    }
    ASSERT_EQ(0, blk_cnt_info.meta_blk_.used_cnt_);
    ASSERT_EQ(0, cache_stat.phy_blk_stat().meta_blk_used_cnt_);
    ASSERT_EQ(blk_cnt_info.meta_blk_.min_cnt_, blk_cnt_info.meta_blk_.hold_cnt_);
    ASSERT_EQ(blk_cnt_info.meta_blk_.min_cnt_, cache_stat.phy_blk_stat().meta_blk_cnt_);
  }

  // 4. reorgan_block
  {
    // allocate all reorgan_block
    const int64_t reorgan_blk_cnt = blk_cnt_info.data_blk_.max_cnt_;
    int64_t block_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ObArray<int64_t> reorgan_blk_idx_arr;
    for (int64_t i = 0; i < reorgan_blk_cnt; i++) {
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_REORGAN_BLK));
      ASSERT_EQ(OB_SUCCESS, reorgan_blk_idx_arr.push_back(block_idx));
      ASSERT_EQ(true, phy_blk_handle.is_valid());
      phy_blk_handle.get_ptr()->is_sealed_ = true;
      phy_blk_handle.reset();
    }
    ASSERT_LT(blk_cnt_info.data_blk_.min_cnt_, blk_cnt_info.data_blk_.hold_cnt_);
    ASSERT_EQ(blk_cnt_info.data_blk_.max_cnt_, blk_cnt_info.data_blk_.hold_cnt_);
    ASSERT_EQ(reorgan_blk_cnt, blk_cnt_info.data_blk_.used_cnt_);
    ASSERT_EQ(reorgan_blk_cnt, cache_stat.phy_blk_stat().data_blk_used_cnt_);
    ASSERT_EQ(reorgan_blk_cnt, blk_cnt_info.data_blk_.hold_cnt_);
    ASSERT_EQ(reorgan_blk_cnt, cache_stat.phy_blk_stat().data_blk_cnt_);

    int64_t shared_blk_used_cnt = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;
    ASSERT_EQ(shared_blk_used_cnt, blk_cnt_info.shared_blk_used_cnt_);
    ASSERT_EQ(shared_blk_used_cnt, cache_stat.phy_blk_stat().shared_blk_used_cnt_);
    {
      ASSERT_EQ(OB_EAGAIN, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_REORGAN_BLK));
    }

    // 8. free all reorgan_block
    bool succ_free = false;
    for (int64_t i = 0; i < reorgan_blk_cnt; i++) {
      succ_free = false;
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(reorgan_blk_idx_arr.at(i)));
      ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(reorgan_blk_idx_arr.at(i), succ_free));
      ASSERT_EQ(true, succ_free);
    }
    ASSERT_EQ(0, blk_cnt_info.data_blk_.used_cnt_);
    ASSERT_EQ(0, cache_stat.phy_blk_stat().data_blk_used_cnt_);
    ASSERT_EQ(blk_cnt_info.data_blk_.min_cnt_, blk_cnt_info.data_blk_.hold_cnt_);
    ASSERT_EQ(blk_cnt_info.data_blk_.min_cnt_, cache_stat.phy_blk_stat().data_blk_cnt_);
  }
}

TEST_F(TestSSPhysicalBlockManager, alloc_and_reuse)
{
  LOG_INFO("TEST_CASE: start alloc_and_reuse");
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;

  int64_t block_idx = -1;
  ObSSPhyBlockHandle phy_blk_handle;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
  ASSERT_NE(-1, block_idx);
  ASSERT_EQ(true, phy_blk_handle.is_valid());

  block_idx = -1;
  phy_blk_handle.reset();
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_META_BLK));
  ASSERT_NE(-1, block_idx);
  ASSERT_EQ(true, phy_blk_handle.is_valid());

  block_idx = -1;
  phy_blk_handle.reset();
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
  ASSERT_NE(-1, block_idx);
  ASSERT_EQ(true, phy_blk_handle.is_valid());

  block_idx = -1;
  phy_blk_handle.reset();
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_REORGAN_BLK));
  ASSERT_NE(-1, block_idx);
  ASSERT_EQ(true, phy_blk_handle.is_valid());

  ASSERT_EQ(2, blk_cnt_info.data_blk_.used_cnt_);
  ASSERT_EQ(1, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(1, blk_cnt_info.meta_blk_.used_cnt_);
  ASSERT_EQ(blk_cnt_info.shared_blk_used_cnt_, blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_);

  blk_cnt_info.reuse();
  ASSERT_EQ(0, blk_cnt_info.data_blk_.used_cnt_);
  ASSERT_EQ(0, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(0, blk_cnt_info.meta_blk_.used_cnt_);
  ASSERT_EQ(blk_cnt_info.shared_blk_used_cnt_, blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_);
}

TEST_F(TestSSPhysicalBlockManager, resize_file_size)
{
  LOG_INFO("TEST_CASE: start resize_file_size");
  // 1. total_block count is less than count of a sub_arr
  int64_t total_block_cnt = ObSSPhysicalBlockManager::PHY_BLOCK_SUB_ARR_SIZE / sizeof(ObSSPhysicalBlock) - 1;
  int64_t file_size = DEFAULT_BLOCK_SIZE * total_block_cnt;
  ObSSMicroCacheStat cache_stat;
  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(DEFAULT_BLOCK_SIZE, ObMemAttr(MTL_ID(), "test"), 1L << 30));
  ObSSPhysicalBlockManager phy_blk_mgr(cache_stat, allocator);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.init(MTL_ID(), file_size, DEFAULT_BLOCK_SIZE));
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  int64_t max_data_cnt = blk_cnt_info.micro_data_blk_max_cnt();
  int64_t max_phy_ckpt_cnt = blk_cnt_info.phy_ckpt_blk_cnt_;

  ObArray<int64_t> phy_blk_idxs;
  ObArray<ObSSPhysicalBlock *> phy_blocks;
  for (int64_t i = 0; i < max_data_cnt; i++) {
    int64_t block_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
    ASSERT_EQ(true, phy_blk_handle.is_valid());
    phy_blk_handle.get_ptr()->is_sealed_ = true;
    ASSERT_EQ(OB_SUCCESS, phy_blocks.push_back(phy_blk_handle.get_ptr()));
    ASSERT_EQ(OB_SUCCESS, phy_blk_idxs.push_back(block_idx));
  }
  for (int64_t i = 0; i < max_phy_ckpt_cnt; ++i) {
    int64_t block_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
    ASSERT_EQ(true, phy_blk_handle.is_valid());
    phy_blk_handle.get_ptr()->is_sealed_ = true;
    ASSERT_EQ(OB_SUCCESS, phy_blocks.push_back(phy_blk_handle.get_ptr()));
    ASSERT_EQ(OB_SUCCESS, phy_blk_idxs.push_back(block_idx));
  }

  // 2. resize file_size
  const int64_t old_min_data_blk_cnt = blk_cnt_info.data_blk_.min_cnt_;
  const int64_t old_max_data_blk_cnt = blk_cnt_info.data_blk_.max_cnt_;
  const int64_t old_used_data_blk_cnt = blk_cnt_info.data_blk_.used_cnt_;
  const int64_t old_min_micro_blk_cnt = blk_cnt_info.meta_blk_.min_cnt_;
  const int64_t old_max_micro_blk_cnt = blk_cnt_info.meta_blk_.max_cnt_;
  const int64_t old_micro_blk_cnt = blk_cnt_info.meta_blk_.used_cnt_;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.format_ss_super_block(SS_PERSIST_MICRO_CKPT_SPLIT_CNT));
  total_block_cnt += ObSSPhysicalBlockManager::PHY_BLOCK_SUB_ARR_SIZE / sizeof(ObSSPhysicalBlock);
  file_size = DEFAULT_BLOCK_SIZE * total_block_cnt;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.resize_file_size(file_size, DEFAULT_BLOCK_SIZE));
  ASSERT_LT(old_min_data_blk_cnt, blk_cnt_info.data_blk_.min_cnt_);
  ASSERT_LT(old_max_data_blk_cnt, blk_cnt_info.data_blk_.max_cnt_);
  ASSERT_EQ(old_used_data_blk_cnt, blk_cnt_info.data_blk_.used_cnt_);
  ASSERT_LE(old_min_micro_blk_cnt, blk_cnt_info.meta_blk_.min_cnt_);
  ASSERT_LT(old_max_micro_blk_cnt, blk_cnt_info.meta_blk_.max_cnt_);
  ASSERT_EQ(old_micro_blk_cnt, blk_cnt_info.meta_blk_.used_cnt_);
  ASSERT_LE(blk_cnt_info.data_blk_.min_cnt_, blk_cnt_info.data_blk_.hold_cnt_);
  ASSERT_EQ(blk_cnt_info.meta_blk_.min_cnt_, blk_cnt_info.meta_blk_.hold_cnt_);
  ASSERT_EQ(blk_cnt_info.shared_blk_used_cnt_, blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_);
  ASSERT_EQ(blk_cnt_info.shared_blk_used_cnt_, cache_stat.phy_blk_stat().shared_blk_used_cnt_);
  ASSERT_EQ(blk_cnt_info.shared_blk_cnt_, cache_stat.phy_blk_stat().shared_blk_cnt_);
  ASSERT_EQ(blk_cnt_info.data_blk_.hold_cnt_, cache_stat.phy_blk_stat().data_blk_cnt_);
  ASSERT_EQ(blk_cnt_info.meta_blk_.hold_cnt_, cache_stat.phy_blk_stat().meta_blk_cnt_);

  // 3. alloc more data block after resize
  int64_t new_max_data_cnt = blk_cnt_info.micro_data_blk_max_cnt();
  ASSERT_LT(max_data_cnt, new_max_data_cnt);
  int64_t new_max_phy_ckpt_cnt = blk_cnt_info.phy_ckpt_blk_cnt_;
  ASSERT_LE(max_phy_ckpt_cnt, new_max_phy_ckpt_cnt);
  for (int64_t i = max_data_cnt; i < new_max_data_cnt; i++) {
    int64_t block_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
    ASSERT_EQ(true, phy_blk_handle.is_valid());
    phy_blk_handle.get_ptr()->is_sealed_ = true;
    ASSERT_EQ(OB_SUCCESS, phy_blocks.push_back(phy_blk_handle.get_ptr()));
    ASSERT_EQ(OB_SUCCESS, phy_blk_idxs.push_back(block_idx));
  }
  for (int64_t i = max_phy_ckpt_cnt; i < new_max_phy_ckpt_cnt; ++i) {
    int64_t block_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
    ASSERT_EQ(true, phy_blk_handle.is_valid());
    phy_blk_handle.get_ptr()->is_sealed_ = true;
    ASSERT_EQ(OB_SUCCESS, phy_blocks.push_back(phy_blk_handle.get_ptr()));
    ASSERT_EQ(OB_SUCCESS, phy_blk_idxs.push_back(block_idx));
  }

  // 4. check the original phy_block's ptr not changed
  for (int64_t i = 0; i < max_data_cnt + max_phy_ckpt_cnt; i++) {
    const int64_t block_idx = phy_blk_idxs.at(i);
    ObSSPhyBlockHandle handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(block_idx, handle));
    ASSERT_EQ(true, handle.is_valid());
    ASSERT_EQ(phy_blocks.at(i), handle.get_ptr());
  }

  // 5. check handle
  ObSSPhyBlockHandle handle1;
  handle1.set_ptr(phy_blocks.at(0));
  ASSERT_EQ(2, handle1.ptr_->ref_cnt_);
  ObSSPhyBlockHandle handle2;
  ASSERT_EQ(OB_SUCCESS, handle2.assign(handle1));
  ASSERT_EQ(3, handle2.ptr_->ref_cnt_);
  handle2.reset();
  handle1.reset();

  // 6. free all allocated phy_blocks
  bool succ_free = false;
  for (int64_t i = 0; i < phy_blk_idxs.count(); i++) {
    succ_free = false;
    const int64_t block_idx = phy_blk_idxs.at(i);
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(block_idx));
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(block_idx, succ_free));
    ASSERT_EQ(true, succ_free);
  }
  ASSERT_EQ(0, blk_cnt_info.data_blk_.used_cnt_);
  ASSERT_EQ(0, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_EQ(0, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(0, cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);
}

TEST_F(TestSSPhysicalBlockManager, parallel_alloc_block)
{
  LOG_INFO("TEST_CASE: start parallel_alloc_block");
  const static uint64_t FILE_SIZE = (1L << 32);
  ObSSMicroCacheStat cache_stat;
  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(DEFAULT_BLOCK_SIZE, ObMemAttr(MTL_ID(), "test"), 1L << 30));
  ObSSPhysicalBlockManager phy_blk_mgr(cache_stat, allocator);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.init(MTL_ID(), FILE_SIZE, DEFAULT_BLOCK_SIZE));
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;

  const int64_t max_reorgan_blk_cnt = blk_cnt_info.reorgan_blk_cnt_ * 2;
  const int64_t max_data_blk_cnt = blk_cnt_info.shared_blk_cnt_ * (SS_MIN_MICRO_DATA_BLK_CNT_PCT + 5) / 100 - max_reorgan_blk_cnt;    // 85%
  const int64_t max_meta_blk_cnt = blk_cnt_info.shared_blk_cnt_ * (SS_MIN_MICRO_META_BLK_CNT_PCT + 5) / 100;  // 9%
  const int64_t data_blk_thread_num = 5;
  const int64_t micro_ckpt_blk_thread_num = 5;
  const int64_t phy_ckpt_blk_thread_num = 1;
  const int64_t reorgan_blk_thread_num = 1;
  int64_t data_blk_cnt = max_data_blk_cnt / data_blk_thread_num;
  int64_t meta_blk_cnt = max_meta_blk_cnt / micro_ckpt_blk_thread_num;
  int64_t phy_ckpt_blk_cnt = blk_cnt_info.phy_ckpt_blk_cnt_ / phy_ckpt_blk_thread_num;
  int64_t reorgan_blk_cnt = max_reorgan_blk_cnt / reorgan_blk_thread_num;

  // 1. multiple threads alloc and free phy_block
  //    tid 0~4: alloc data_blk
  //    tid 5~9: alloc meta_blk
  //    tid 10: alloc phy_ckpt_blk
  //    tid 11: alloc reorgan_blk
  {
    int64_t fail_cnt = 0;
    ObTenantBase* tenant_base = MTL_CTX();
    auto test_func = [&](const int64_t idx) {
      ObTenantEnv::set_tenant(tenant_base);
      int ret = OB_SUCCESS;
      int64_t blk_cnt = 0;
      ObSSPhyBlockType blk_type = ObSSPhyBlockType::SS_INVALID_TYPE;
      if (idx < 5) {
        blk_cnt = data_blk_cnt;
        blk_type = ObSSPhyBlockType::SS_MICRO_DATA_BLK;
      } else if (idx < 10) {
        blk_cnt = meta_blk_cnt;
        blk_type = ObSSPhyBlockType::SS_MICRO_META_BLK;
      } else if (idx < 11) {
        blk_cnt = phy_ckpt_blk_cnt;
        blk_type = ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK;
      } else {
        blk_cnt = reorgan_blk_cnt;
        blk_type = ObSSPhyBlockType::SS_REORGAN_BLK;
      }

      int64_t block_idx = -1;
      ObArray<int64_t> block_idx_arr;
      bool succ_free = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < blk_cnt; i++) {
        ObSSPhyBlockHandle phy_block_handle;
        if (OB_FAIL(phy_blk_mgr.alloc_block(block_idx, phy_block_handle, blk_type))) {
          LOG_WARN("fail to allocate block", KR(ret));
        } else if (OB_FAIL(block_idx_arr.push_back(block_idx))) {
          LOG_WARN("fail to push back block_idx", KR(ret), K(block_idx));
        } else {
          phy_block_handle.get_ptr()->is_sealed_ = true;
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < blk_cnt; i++) {
        block_idx = block_idx_arr[i];
        if (OB_FAIL(phy_blk_mgr.add_reusable_block(block_idx))) {
          LOG_WARN("fail to add block into reusable_set", KR(ret), K(block_idx));
        } else if (OB_FAIL(phy_blk_mgr.free_block(block_idx, succ_free)) || (succ_free != true)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to allocate block", KR(ret), K(block_idx), K(succ_free));
        }
      }

      if (OB_FAIL(ret)) {
        ATOMIC_INC(&fail_cnt);
      }
    };

    const int64_t thread_num = 12;
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

  // 2. check stat
  {
    ASSERT_EQ(0, blk_cnt_info.data_blk_.used_cnt_);
    ASSERT_EQ(0, blk_cnt_info.meta_blk_.used_cnt_);
    ASSERT_EQ(0, blk_cnt_info.phy_ckpt_blk_used_cnt_);
    ASSERT_EQ(blk_cnt_info.data_blk_.min_cnt_, blk_cnt_info.data_blk_.hold_cnt_);
    ASSERT_EQ(blk_cnt_info.meta_blk_.min_cnt_, blk_cnt_info.meta_blk_.hold_cnt_);
    ASSERT_EQ(0, cache_stat.phy_blk_stat().data_blk_used_cnt_);
    ASSERT_EQ(0, cache_stat.phy_blk_stat().meta_blk_used_cnt_);
    ASSERT_EQ(0, cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);
    ASSERT_EQ(blk_cnt_info.data_blk_.hold_cnt_, cache_stat.phy_blk_stat().data_blk_cnt_);
    ASSERT_EQ(blk_cnt_info.meta_blk_.hold_cnt_, cache_stat.phy_blk_stat().meta_blk_cnt_);
    const int64_t shared_blk_used_cnt = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;
    ASSERT_EQ(shared_blk_used_cnt, blk_cnt_info.shared_blk_used_cnt_);
    ASSERT_EQ(shared_blk_used_cnt, cache_stat.phy_blk_stat().shared_blk_used_cnt_);
  }
}

/* Multiple threads allocate/free data_block for multiple rounds and check reuse version. */
TEST_F(TestSSPhysicalBlockManager, parallel_allocate_block_and_check_reuse_version)
{
  LOG_INFO("TEST_CASE: start parallel_allocate_block_and_check_reuse_version");
  const static uint64_t FILE_SIZE = (1L << 40);
  ObSSMicroCacheStat cache_stat;
  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(DEFAULT_BLOCK_SIZE, ObMemAttr(MTL_ID(), "test"), 1L << 30));
  ObSSPhysicalBlockManager phy_blk_mgr(cache_stat, allocator);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.init(MTL_ID(), FILE_SIZE, DEFAULT_BLOCK_SIZE));
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;

  const int32_t thread_cnt = 2;
  int64_t data_blk_cnt = blk_cnt_info.micro_data_blk_max_cnt() / thread_cnt;
  data_blk_cnt = MIN(2000, data_blk_cnt);
  int64_t phy_ckpt_blk_cnt = 0;
  int64_t micro_ckpt_blk_cnt = 0;
  int64_t reorgan_blk_cnt = 0;
  int64_t ori_total_reuse_version = 0;
  for (int64_t i = 2; i < blk_cnt_info.total_blk_cnt_; i++) {
    ObSSPhyBlockHandle handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(i, handle));
    handle.get_ptr()->reuse_version_ = 1;
    ++ori_total_reuse_version;
  }

  // multiple threads alloc and free
  {
    int64_t fail_cnt = 0;
    ObTenantBase *tenant_base = MTL_CTX();

    auto test_func = [&](const int64_t idx) {
      ObTenantEnv::set_tenant(tenant_base);
      int ret = OB_SUCCESS;
      int64_t block_idx = -1;
      common::ObArray<int64_t> block_idx_arr;
      bool succ_free = false;
      for (int64_t epoch = 0; OB_SUCC(ret) && epoch < 10; epoch++) {
        for (int64_t i = 0; OB_SUCC(ret) && i < data_blk_cnt; i++) {
          ObSSPhyBlockHandle phy_block_handle;
          if (OB_FAIL(phy_blk_mgr.alloc_block(block_idx, phy_block_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK))) {
            LOG_WARN("fail to allocate block", KR(ret));
          } else if (OB_FAIL(block_idx_arr.push_back(block_idx))) {
            LOG_WARN("fail to push back block_idx", KR(ret), K(block_idx));
          } else {
            phy_block_handle.get_ptr()->is_sealed_ = true;
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < block_idx_arr.count(); i++) {
          block_idx = block_idx_arr[i];
          if (OB_FAIL(phy_blk_mgr.add_reusable_block(block_idx))) {
            LOG_WARN("fail to add block into reusable_set", KR(ret), K(block_idx));
          } else if (OB_FAIL(phy_blk_mgr.free_block(block_idx, succ_free)) && (succ_free != true)) {
            LOG_WARN("fail to allocate block", KR(ret), K(block_idx), K(succ_free));
          }
        }
        block_idx_arr.reuse();
      }
      if (OB_FAIL(ret)) {
        ATOMIC_INC(&fail_cnt);
      }
    };

    std::vector<std::thread> ths;
    for (int64_t i = 0; i < thread_cnt; ++i) {
      std::thread th(test_func, i);
      ths.push_back(std::move(th));
    }
    for (int64_t i = 0; i < thread_cnt; ++i) {
      ths[i].join();
    }
    ASSERT_EQ(0, fail_cnt);
  }

  int64_t block_idx = -1;
  const int64_t data_blk_used_cnt = blk_cnt_info.data_blk_.used_cnt_;
  ASSERT_EQ(0, data_blk_used_cnt);
  int64_t total_reuse_version = 0;
  for (int64_t i = 2; i < blk_cnt_info.total_blk_cnt_; i++) {
    ObSSPhyBlockHandle handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(i, handle));
    total_reuse_version += handle.get_ptr()->reuse_version_;
  }
  ASSERT_EQ(data_blk_cnt * 10 * thread_cnt, total_reuse_version - ori_total_reuse_version);
}


/* Multiple threads allocate cache_data_blocks and resize file size in parallel. */
TEST_F(TestSSPhysicalBlockManager, parallel_allocate_block_and_resize)
{
  LOG_INFO("TEST_CASE: start parallel_allocate_block_and_resize");
  const static uint64_t FILE_SIZE = (1L << 32);
  ObSSMicroCacheStat cache_stat;
  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(DEFAULT_BLOCK_SIZE, ObMemAttr(MTL_ID(), "test"), 1L << 30));
  ObSSPhysicalBlockManager phy_blk_mgr(cache_stat, allocator);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.init(MTL_ID(), FILE_SIZE, DEFAULT_BLOCK_SIZE));
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.format_ss_super_block(SS_PERSIST_MICRO_CKPT_SPLIT_CNT));

  const int64_t origin_file_size = phy_blk_mgr.cache_file_size_;
  const int64_t thread_num = 10;
  int64_t data_blk_cnt = phy_blk_mgr.blk_cnt_info_.micro_data_blk_max_cnt() / thread_num;

  {
    int64_t fail_cnt = 0;
    ObTenantBase *tenant_base = MTL_CTX();
    auto test_func = [&](const int64_t idx) {
      ObTenantEnv::set_tenant(tenant_base);
      int ret = OB_SUCCESS;
      int64_t block_idx = -1;
      ObArray<ObSSPhysicalBlock *> phy_block_arr;
      ObArray<int64_t> block_idx_arr;
      if (idx >= 5) {
        for (int64_t i = 0; OB_SUCC(ret) && i < data_blk_cnt; i++) {
          ObSSPhyBlockHandle phy_block_handle;
          if (OB_FAIL(phy_blk_mgr.alloc_block(block_idx, phy_block_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK))) {
            LOG_WARN("fail to allocate block", KR(ret), K(block_idx));
          } else if (OB_FAIL(block_idx_arr.push_back(block_idx))) {
            LOG_WARN("fail to push back block idx", KR(ret), K(block_idx));
          } else if (OB_FAIL(phy_block_arr.push_back(phy_block_handle.get_ptr()))) {
            LOG_WARN("fail to push back phy_block", KR(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < block_idx_arr.count(); i++) {
          block_idx = block_idx_arr[i];
          ObSSPhyBlockHandle phy_block_handle;
          if (OB_FAIL(phy_blk_mgr.get_block_handle(block_idx, phy_block_handle))) {
            LOG_WARN("fail to get block handle", KR(ret), K(block_idx));
          } else if (phy_block_handle.get_ptr() != phy_block_arr[i]) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("phy_block not match", KR(ret), KP(phy_block_handle.get_ptr()), KP(phy_block_arr[i]));
          }
        }
      } else {
        int64_t new_file_size = (idx + 1) * origin_file_size;
        phy_blk_mgr.resize_file_size(new_file_size, DEFAULT_BLOCK_SIZE); // may fail for resizing smaller, ignore it.
      }

      if (OB_FAIL(ret)) {
        ATOMIC_INC(&fail_cnt);
      }
    };

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

  ASSERT_EQ(data_blk_cnt * 5, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
  ASSERT_EQ(origin_file_size * 5, phy_blk_mgr.cache_file_size_);
}

/*
  Test two scenarios:
    1: Both super_blocks are correct, read the latest one
    2: One of the super_blocks is correct and the other is incorrect, read the correct one
*/
TEST_F(TestSSPhysicalBlockManager, double_write_super_blk)
{
  LOG_INFO("TEST_CASE: start double_write_super_blk");
  const uint64_t tenant_id = MTL_ID();
  const static uint64_t FILE_SIZE = (1L << 32);
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
  ObSSMicroCacheStat cache_stat;
  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(DEFAULT_BLOCK_SIZE, ObMemAttr(MTL_ID(), "test"), 1L << 30));
  ObSSPhysicalBlockManager phy_blk_mgr(cache_stat, allocator);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.init(MTL_ID(), FILE_SIZE, DEFAULT_BLOCK_SIZE));
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.format_ss_super_block(SS_PERSIST_MICRO_CKPT_SPLIT_CNT));

  const int64_t align_size = SS_MEM_BUF_ALIGNMENT;
  char *buf = static_cast<char *>(ob_malloc_align(SS_MEM_BUF_ALIGNMENT, align_size, ObMemAttr(MTL_ID(), "test")));
  ASSERT_NE(nullptr, buf);

  // Scenario 1
  const int64_t old_cache_file_size = phy_blk_mgr.cache_file_size_ * 2;
  const int64_t new_cache_file_size = phy_blk_mgr.cache_file_size_ * 4;
  ObSSMicroCacheSuperBlk old_super_blk(tenant_id, old_cache_file_size, SS_PERSIST_MICRO_CKPT_SPLIT_CNT);
  ObSSMicroCacheSuperBlk new_super_blk(tenant_id, new_cache_file_size, SS_PERSIST_MICRO_CKPT_SPLIT_CNT);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_ss_super_block(old_super_blk));

  new_super_blk.modify_time_us_ = old_super_blk.modify_time_us_ + 888888;
  ASSERT_EQ(OB_SUCCESS, serialize_super_block(buf, align_size, new_super_blk));
  int64_t write_size = 0;
  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->pwrite_cache_block(0, align_size, buf, write_size));

  ObSSMicroCacheSuperBlk super_blk;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.read_ss_super_block(super_blk));
  check_super_block_identical(phy_blk_mgr.super_blk_, new_super_blk);

  // Scenario 2
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_ss_super_block(new_super_blk));
  const int64_t fake_cache_file_size = phy_blk_mgr.cache_file_size_ * 8;
  ObSSMicroCacheSuperBlk fake_super_blk(tenant_id, new_cache_file_size, SS_PERSIST_MICRO_CKPT_SPLIT_CNT);
  fake_super_blk.modify_time_us_ = new_super_blk.modify_time_us_ + 888888;

  ASSERT_EQ(OB_SUCCESS, serialize_super_block(buf, align_size, fake_super_blk));
  buf[fake_super_blk.get_serialize_size() - 3] = '#'; // simulate data error
  ObSSMicroCacheSuperBlk tmp_super_blk;
  ASSERT_NE(OB_SUCCESS, deserialize_super_block(buf, align_size, tmp_super_blk));

  write_size = 0;
  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->pwrite_cache_block(0, align_size, buf, write_size));
  super_blk.reset();
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.read_ss_super_block(super_blk));
  check_super_block_identical(phy_blk_mgr.super_blk_, new_super_blk);

  ob_free_align(buf);
}

TEST_F(TestSSPhysicalBlockManager, alloc_all_phy_block)
{
  LOG_INFO("TEST_CASE: start alloc_all_phy_block");
  int ret = OB_SUCCESS;

  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSMicroCacheSuperBlk &super_blk = phy_blk_mgr.super_blk_;
  ObSSPhyBlockCountInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  ASSERT_EQ(1, super_blk.get_ckpt_split_cnt());
  LOG_INFO("TEST: check blk_cnt_info", K(blk_cnt_info));

  ObArray<ObSSPhyBlockHandle> alloc_blk_handles;
  // 1. alloc all micro_data_blk
  bool finish_alloc = false;
  while (OB_SUCC(ret) && !finish_alloc) {
    int64_t blk_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ret = phy_blk_mgr.alloc_block(blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK);
    if (OB_FAIL(ret)) {
      ASSERT_EQ(OB_EAGAIN, ret);
      ret = OB_SUCCESS;
      finish_alloc = true;
    } else {
      ASSERT_EQ(true, phy_blk_handle.is_valid());
      ASSERT_EQ(OB_SUCCESS, alloc_blk_handles.push_back(phy_blk_handle));
      phy_blk_handle.get_ptr()->valid_len_ = 100;
    }
  }
  ASSERT_EQ(false, blk_cnt_info.has_free_blk(ObSSPhyBlockType::SS_MICRO_DATA_BLK));

  // 2. alloc all reorgan_blk
  finish_alloc = false;
  int64_t reorgan_blk_start_idx = alloc_blk_handles.count();
  while (OB_SUCC(ret) && !finish_alloc) {
    int64_t blk_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ret = phy_blk_mgr.alloc_block(blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_REORGAN_BLK);
    if (OB_FAIL(ret)) {
      ASSERT_EQ(OB_EAGAIN, ret);
      ret = OB_SUCCESS;
      finish_alloc = true;
    } else {
      ASSERT_EQ(true, phy_blk_handle.is_valid());
      ASSERT_EQ(OB_SUCCESS, alloc_blk_handles.push_back(phy_blk_handle));
      phy_blk_handle.get_ptr()->valid_len_ = 100;
    }
  }
  ASSERT_EQ(false, blk_cnt_info.has_free_blk(ObSSPhyBlockType::SS_REORGAN_BLK));
  int64_t reorgan_blk_end_idx = alloc_blk_handles.count();

  // 3. alloc all micro_meta_blk
  const int64_t meta_blk_cnt = blk_cnt_info.meta_blk_.free_blk_cnt() / 2;
  for (int64_t i = 0; i < meta_blk_cnt; ++i) {
    int64_t blk_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_META_BLK));
    ASSERT_NE(-1, blk_idx);
    ASSERT_EQ(true, phy_blk_handle.is_valid());
    ASSERT_EQ(OB_SUCCESS, alloc_blk_handles.push_back(phy_blk_handle));
    ASSERT_EQ(OB_SUCCESS, super_blk.micro_ckpt_info_.micro_ckpt_used_blks_.at(0).push_back(blk_idx));
    phy_blk_handle.get_ptr()->valid_len_ = 0;
  }
  int64_t first_micro_entry = super_blk.micro_ckpt_info_.micro_ckpt_used_blks_.at(0).at(0);
  ASSERT_EQ(OB_SUCCESS, super_blk.micro_ckpt_info_.micro_ckpt_entries_.push_back(ObSSMicroCkptEntryItem(true, first_micro_entry)));
  ASSERT_EQ(meta_blk_cnt, blk_cnt_info.meta_blk_.used_cnt_);

  // 4. alloc all info_ckpt_blk
  const int64_t info_blk_cnt = blk_cnt_info.phy_ckpt_blk_cnt_ / 2;
  for (int64_t i = 0; i < info_blk_cnt; ++i) {
    int64_t blk_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
    ASSERT_NE(-1, blk_idx);
    ASSERT_EQ(true, phy_blk_handle.is_valid());
    ASSERT_EQ(OB_SUCCESS, alloc_blk_handles.push_back(phy_blk_handle));
    ASSERT_EQ(OB_SUCCESS, super_blk.blk_ckpt_info_.blk_ckpt_used_blks_.push_back(blk_idx));
    phy_blk_handle.get_ptr()->valid_len_ = 0;
  }
  super_blk.blk_ckpt_info_.blk_ckpt_entry_ = super_blk.blk_ckpt_info_.blk_ckpt_used_blks_.at(0);
  ASSERT_EQ(info_blk_cnt, blk_cnt_info.phy_ckpt_blk_used_cnt_);

  // 5. mock after read checkpoint, we need to update blk_state
  for (int64_t i = 0; i < alloc_blk_handles.count(); ++i) {
    alloc_blk_handles.at(i).get_ptr()->is_free_ = 1;
  }
  for (int64_t i = SS_SUPER_BLK_COUNT; i < blk_cnt_info.total_blk_cnt_; ++i) {
    phy_blk_mgr.free_bitmap_->set(i, true);
  }
  blk_cnt_info.reuse();
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_block_state_for_restart());
  for (int64_t i = 0; i < alloc_blk_handles.count(); ++i) {
    ASSERT_EQ(0, alloc_blk_handles.at(i).get_ptr()->is_free_);
    ASSERT_EQ(1, alloc_blk_handles.at(i).get_ptr()->is_sealed_);
  }

  {
    int64_t blk_idx = -1;
    ObSSPhyBlockHandle phy_blk_handle;
    ASSERT_NE(OB_SUCCESS, phy_blk_mgr.alloc_block(blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK));
    ASSERT_NE(OB_SUCCESS, phy_blk_mgr.alloc_block(blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_REORGAN_BLK));
    // Cuz there still exists free ckpt phy_block
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_META_BLK));
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_PHY_BLK_CKPT_BLK));
  }
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_physical_block_manager.log*");
  OB_LOGGER.set_file_name("test_ss_physical_block_manager.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}