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
#include "mittest/mtlenv/mock_tenant_module_env.h"
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
  int serialize_super_block(char *buf, const int64_t buf_size, const ObSSMicroCacheSuperBlock &super_block);
  int deserialize_super_block(char *buf, const int64_t buf_size, ObSSMicroCacheSuperBlock &super_block);
  void check_super_block_identical(
      const ObSSMicroCacheSuperBlock &super_block1, const ObSSMicroCacheSuperBlock &super_block2);
  void check_phy_block_reserve_cnt(const int64_t expect_cnt, const ObSSPhyBlockType type);
public:
  class TestSSPhyBlockMgrThread : public Threads {
  public:
    enum class TestParallelType
    {
      TEST_PARALLEL_ALLOCATE_BLOCK,
      TEST_PARALLEL_ALLOCATE_BLOCK_AND_CHECK_REUSE_VERSION,
      TEST_PARALLEL_ALLOCATE_BLOCK_AND_RESIZE,
    };

  public:
    TestSSPhyBlockMgrThread(ObTenantBase *tenant_base, ObSSPhysicalBlockManager *phy_blk_mgr, int64_t normal_blk_cnt,
        int64_t phy_ckpt_blk_cnt, int64_t micro_ckpt_blk_cnt, int64_t reorgan_blk_cnt, TestParallelType type)
        : tenant_base_(tenant_base),
          phy_blk_mgr_(phy_blk_mgr),
          data_blk_cnt_(normal_blk_cnt),
          phy_ckpt_blk_cnt_(phy_ckpt_blk_cnt),
          micro_ckpt_blk_cnt_(micro_ckpt_blk_cnt),
          reorgan_blk_cnt_(reorgan_blk_cnt),
          ori_total_file_size_(phy_blk_mgr->total_file_size_),
          type_(type),
          fail_cnt_(0)
    {}

    void run(int64_t idx) final
    {
      ObTenantEnv::set_tenant(tenant_base_);
      if (type_ == TestParallelType::TEST_PARALLEL_ALLOCATE_BLOCK) {
        parallel_allocate_block(idx);
      } else if (type_ == TestParallelType::TEST_PARALLEL_ALLOCATE_BLOCK_AND_CHECK_REUSE_VERSION) {
        parallel_allocate_block_and_check_reuse_version(idx);
      } else if (type_ == TestParallelType::TEST_PARALLEL_ALLOCATE_BLOCK_AND_RESIZE) {
        parallel_allocate_block_and_resize(idx);
      }
    }

    int64_t get_fail_cnt()
    {
      return ATOMIC_LOAD(&fail_cnt_);
    }

  private:
    int parallel_allocate_block(int64_t idx)
    {
      int ret = OB_SUCCESS;
      int64_t blk_cnt = 0;
      ObSSPhyBlockType blk_type = ObSSPhyBlockType::SS_INVALID_BLK_TYPE;

      if (idx < 5) {
        blk_cnt = data_blk_cnt_;
        blk_type = ObSSPhyBlockType::SS_CACHE_DATA_BLK;
      } else if (idx < 10) {
        blk_cnt = micro_ckpt_blk_cnt_;
        blk_type = ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK;
      } else if (idx < 11) {
        blk_cnt = phy_ckpt_blk_cnt_;
        blk_type = ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK;
      } else {
        blk_cnt = reorgan_blk_cnt_;
        blk_type = ObSSPhyBlockType::SS_REORGAN_BLK;
      }

      int64_t block_idx = -1;
      ObArray<int64_t> block_idx_arr;
      bool succ_free = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < blk_cnt; i++) {
        ObSSPhysicalBlockHandle phy_block_handle;
        if (OB_FAIL(phy_blk_mgr_->alloc_block(block_idx, phy_block_handle, blk_type))) {
          LOG_WARN("fail to allocate block", KR(ret));
        } else if (OB_FAIL(block_idx_arr.push_back(block_idx))) {
          LOG_WARN("fail to push back block_idx", KR(ret), K(block_idx));
        } else {
          phy_block_handle.get_ptr()->is_sealed_ = true;
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < blk_cnt; i++) {
        block_idx = block_idx_arr[i];
        if (OB_FAIL(phy_blk_mgr_->add_reusable_block(block_idx))) {
          LOG_WARN("fail to add block into reusable_set", KR(ret), K(block_idx));
        } else if (OB_FAIL(phy_blk_mgr_->free_block(block_idx, succ_free)) || (succ_free != true)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to allocate block", KR(ret), K(block_idx), K(succ_free));
        }
      }

      if (OB_FAIL(ret)) {
        ATOMIC_INC(&fail_cnt_);
      }
      return ret;
    }

    int parallel_allocate_block_and_check_reuse_version(int64_t idx)
    {
      int ret = OB_SUCCESS;
      int64_t block_idx = -1;
      common::ObArray<int64_t> block_idx_arr;
      bool succ_free = false;
      for (int64_t epoch = 0; OB_SUCC(ret) && epoch < 10; epoch++) {
        for (int64_t i = 0; OB_SUCC(ret) && i < data_blk_cnt_; i++) {
          ObSSPhysicalBlockHandle phy_block_handle;
          if (OB_FAIL(
                  phy_blk_mgr_->alloc_block(block_idx, phy_block_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK))) {
            LOG_WARN("fail to allocate block", KR(ret));
          } else if (OB_FAIL(block_idx_arr.push_back(block_idx))) {
            LOG_WARN("fail to push back block_idx", KR(ret), K(block_idx));
          } else {
            phy_block_handle.get_ptr()->is_sealed_ = true;
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < block_idx_arr.count(); i++) {
          block_idx = block_idx_arr[i];
          if (OB_FAIL(phy_blk_mgr_->add_reusable_block(block_idx))) {
            LOG_WARN("fail to add block into reusable_set", KR(ret), K(block_idx));
          } else if (OB_FAIL(phy_blk_mgr_->free_block(block_idx, succ_free)) && (succ_free != true)) {
            LOG_WARN("fail to allocate block", KR(ret), K(block_idx), K(succ_free));
          }
        }
        block_idx_arr.reuse();
      }

      if (OB_FAIL(ret)) {
        ATOMIC_INC(&fail_cnt_);
      }
      return ret;
    }

    int parallel_allocate_block_and_resize(int64_t idx)
    {
      int ret = OB_SUCCESS;
      int64_t block_idx = -1;
      common::ObArray<ObSSPhysicalBlock*> phy_block_arr;
      common::ObArray<int64_t> block_idx_arr;
      if (idx >= 5) {
        for (int64_t i = 0; OB_SUCC(ret) && i < data_blk_cnt_; i++) {
          ObSSPhysicalBlockHandle phy_block_handle;
          if (OB_FAIL(phy_blk_mgr_->alloc_block(block_idx, phy_block_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK))) {
            LOG_WARN("fail to allocate block", KR(ret), K(block_idx));
          } else if (OB_FAIL(block_idx_arr.push_back(block_idx))) {
            LOG_WARN("fail to push back block idx",  KR(ret), K(block_idx));
          } else if (OB_FAIL(phy_block_arr.push_back(phy_block_handle.get_ptr()))) {
            LOG_WARN("fail to push back phy_block", KR(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < block_idx_arr.count(); i++) {
          block_idx = block_idx_arr[i];
          ObSSPhysicalBlockHandle phy_block_handle;
          if (OB_FAIL(phy_blk_mgr_->get_block_handle(block_idx, phy_block_handle))) {
            LOG_WARN("fail to get block handle", KR(ret), K(block_idx));
          } else if (phy_block_handle.get_ptr() != phy_block_arr[i]) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("phy_block not match", KR(ret), KP(phy_block_handle.get_ptr()), KP(phy_block_arr[i]));
          }
        }
      } else {
        int64_t new_file_size = 1L * (idx + 1) * ori_total_file_size_;
        // may fail for resizing smaller, ignore it.
        phy_blk_mgr_->resize_file_size(new_file_size, phy_blk_mgr_->block_size_);
      }

      if (OB_FAIL(ret)) {
        ATOMIC_INC(&fail_cnt_);
      }
      return ret;
    }

  private:
    ObTenantBase *tenant_base_;
    ObSSPhysicalBlockManager *phy_blk_mgr_;
    int64_t data_blk_cnt_;
    int64_t phy_ckpt_blk_cnt_;
    int64_t micro_ckpt_blk_cnt_;
    int64_t reorgan_blk_cnt_;
    int64_t ori_total_file_size_;
    TestParallelType type_;
    int64_t fail_cnt_;
  };
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
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32)));
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
    const ObSSMicroCacheSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  ObSSPhyBlockCommonHeader common_header;
  int64_t pos = common_header.header_size_;
  if (OB_UNLIKELY(nullptr == buf || buf_size <= common_header.header_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(super_block.serialize(buf, buf_size, pos))) {
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
    ObSSMicroCacheSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroCacheSuperBlock tmp_super_blk;
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
  } else {
    super_block = tmp_super_blk;
  }
  return ret;
}

void TestSSPhysicalBlockManager::check_super_block_identical(
    const ObSSMicroCacheSuperBlock &super_block1,
    const ObSSMicroCacheSuperBlock &super_block2)
{
  ASSERT_EQ(super_block1.micro_ckpt_time_us_, super_block2.micro_ckpt_time_us_);
  ASSERT_EQ(super_block1.cache_file_size_, super_block2.cache_file_size_);
  ASSERT_EQ(super_block1.modify_time_us_, super_block2.modify_time_us_);
  ASSERT_EQ(super_block1.micro_ckpt_entry_list_.count(), super_block2.micro_ckpt_entry_list_.count());
  for (int64_t i = 0; i < super_block2.micro_ckpt_entry_list_.count(); ++i) {
    ASSERT_EQ(super_block1.micro_ckpt_entry_list_[i], super_block2.micro_ckpt_entry_list_[i]);
  }
  ASSERT_EQ(super_block1.blk_ckpt_entry_list_.count(), super_block2.blk_ckpt_entry_list_.count());
  for (int64_t i = 0; i < super_block2.blk_ckpt_entry_list_.count(); ++i) {
    ASSERT_EQ(super_block1.blk_ckpt_entry_list_[i], super_block2.blk_ckpt_entry_list_[i]);
  }
}

TEST_F(TestSSPhysicalBlockManager, physical_block)
{
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = phy_blk_mgr.cache_stat_;
  SSPhyBlockCntInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  ASSERT_EQ(true, blk_cnt_info.is_valid());

  ASSERT_LT(0, blk_cnt_info.total_blk_cnt_);
  ASSERT_EQ(blk_cnt_info.total_blk_cnt_, blk_cnt_info.super_blk_cnt_ +
                                         blk_cnt_info.shared_blk_cnt_ +
                                         blk_cnt_info.phy_ckpt_blk_cnt_);
  const int64_t data_blk_min_cnt =
      MAX(blk_cnt_info.reorgan_blk_cnt_, blk_cnt_info.shared_blk_cnt_ * MIN_CACHE_DATA_BLOCK_CNT_PCT / 100);
  const int64_t micro_blk_min_cnt = MAX(2, blk_cnt_info.shared_blk_cnt_ * MIN_MICRO_CKPT_BLOCK_CNT_PCT / 100);
  ASSERT_EQ(data_blk_min_cnt, blk_cnt_info.data_blk_.min_cnt_);
  ASSERT_EQ(data_blk_min_cnt, blk_cnt_info.data_blk_.hold_cnt_);
  ASSERT_EQ(micro_blk_min_cnt, blk_cnt_info.meta_blk_.min_cnt_);
  ASSERT_EQ(micro_blk_min_cnt, blk_cnt_info.meta_blk_.hold_cnt_);
  ASSERT_EQ(blk_cnt_info.shared_blk_cnt_, blk_cnt_info.data_blk_.min_cnt_ + blk_cnt_info.meta_blk_.max_cnt_);
  ASSERT_EQ(blk_cnt_info.shared_blk_cnt_, blk_cnt_info.data_blk_.max_cnt_ + blk_cnt_info.meta_blk_.min_cnt_);
  ASSERT_EQ(data_blk_min_cnt, cache_stat.phy_blk_stat().data_blk_cnt_);
  ASSERT_EQ(micro_blk_min_cnt, cache_stat.phy_blk_stat().meta_blk_cnt_);

  ObSSPhysicalBlockHandle phy_blk_handle;
  int64_t phy_blk_idx = -1;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(phy_blk_idx, phy_blk_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
  ASSERT_LT(0, phy_blk_idx);
  ASSERT_EQ(true, phy_blk_handle.is_valid());
  ASSERT_EQ(1, blk_cnt_info.data_blk_.used_cnt_);
  ASSERT_EQ(1, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  phy_blk_handle()->valid_len_ = 50;
  bool is_empty = false;
  ASSERT_EQ(OB_SUCCESS, phy_blk_handle()->dec_valid_len(phy_blk_idx, -50));
  ASSERT_EQ(true, phy_blk_handle()->is_empty());
  ASSERT_EQ(1, phy_blk_mgr.reusable_set_.size());
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

TEST_F(TestSSPhysicalBlockManager, super_block)
{
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ASSERT_EQ(true, phy_blk_mgr.is_inited_);
  phy_blk_mgr.super_block_.reset();
  ASSERT_EQ(false, phy_blk_mgr.super_block_.is_valid());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.format_ss_super_block());
  ASSERT_EQ(true, phy_blk_mgr.super_block_.is_valid());
  ObSSMicroCacheSuperBlock super_blk;
  ASSERT_EQ(false, super_blk.is_valid());
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_ss_super_block(super_blk));
  ASSERT_EQ(true, super_blk.is_valid());
  super_blk.modify_time_us_ = 1000001;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_ss_super_block(super_blk));
  ASSERT_EQ(true, phy_blk_mgr.super_block_.is_valid());
  ASSERT_EQ(super_blk.modify_time_us_, phy_blk_mgr.super_block_.modify_time_us_);
  ASSERT_EQ(false, super_blk.is_valid_checkpoint());
}

TEST_F(TestSSPhysicalBlockManager, physical_block_manager)
{
  int ret = OB_SUCCESS;
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = phy_blk_mgr.cache_stat_;
  SSPhyBlockCntInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  ASSERT_EQ(true, phy_blk_mgr.is_inited_);

  // 1. allocate all avaliable cache_data_block
  const int64_t data_blk_cnt = blk_cnt_info.cache_limit_blk_cnt();
  ASSERT_LT(0, data_blk_cnt);
  int64_t block_idx = -1;
  ObSSPhysicalBlockHandle phy_blk_handle;
  ObArray<int64_t> data_blk_idx_arr;
  for (int64_t i = 0; OB_SUCC(ret) && i < data_blk_cnt; i++) {
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
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
    // no free cache_data_block to allocate.
    ASSERT_EQ(OB_EAGAIN, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
  }

  // 2. free all cache_data_blk
  bool succ_free = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < data_blk_cnt; i++) {
    succ_free = false;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(data_blk_idx_arr.at(i)));
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(data_blk_idx_arr.at(i), succ_free));
    ASSERT_EQ(true, succ_free);
  }
  ASSERT_EQ(0, blk_cnt_info.data_blk_.used_cnt_);
  ASSERT_EQ(0, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_EQ(blk_cnt_info.data_blk_.min_cnt_, blk_cnt_info.data_blk_.hold_cnt_);
  ASSERT_EQ(blk_cnt_info.data_blk_.min_cnt_, cache_stat.phy_blk_stat().data_blk_cnt_);

  // 3. allocate all phy_block_ckpt_block
  int64_t phy_ckpt_blk_cnt = phy_blk_mgr.blk_cnt_info_.phy_ckpt_blk_cnt_;
  ASSERT_LT(0, phy_ckpt_blk_cnt);
  block_idx = -1;
  ObArray<int64_t> phy_ckpt_blk_idx_arr;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_ckpt_blk_cnt; i++) {
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK));
    ASSERT_EQ(OB_SUCCESS, phy_ckpt_blk_idx_arr.push_back(block_idx));
    ASSERT_EQ(true, phy_blk_handle.is_valid());
    phy_blk_handle.get_ptr()->is_sealed_ = true;
    phy_blk_handle.reset();
  }
  ASSERT_EQ(phy_ckpt_blk_cnt, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(phy_ckpt_blk_cnt, cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);
  {
    // no free ckpt_block to allocate.
    ASSERT_EQ(OB_EAGAIN, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK));
  }

  // 4. free all phy_block_ckpt_block
  succ_free = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_ckpt_blk_cnt; i++) {
    succ_free = false;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(phy_ckpt_blk_idx_arr.at(i)));
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(phy_ckpt_blk_idx_arr.at(i), succ_free));
    ASSERT_EQ(true, succ_free);
  }
  ASSERT_EQ(0, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(0, cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);

  // 5. allocate all micro_meta_ckpt_block
  const int64_t micro_ckpt_blk_cnt = blk_cnt_info.meta_blk_.max_cnt_;
  ASSERT_LT(0, micro_ckpt_blk_cnt);
  block_idx = -1;
  ObArray<int64_t> micro_ckpt_blk_idx_arr;
  for (int64_t i = 0; OB_SUCC(ret) && i < micro_ckpt_blk_cnt; i++) {
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK));
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

  shared_blk_used_cnt = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;
  ASSERT_EQ(shared_blk_used_cnt, blk_cnt_info.shared_blk_used_cnt_);
  ASSERT_EQ(shared_blk_used_cnt, cache_stat.phy_blk_stat().shared_blk_used_cnt_);
  {
    // no free ckpt_block to allocate.
    ASSERT_EQ(OB_EAGAIN, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK));
  }

  // 6. free all micro_meta_blockckpt_block
  succ_free = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < micro_ckpt_blk_cnt; i++) {
    succ_free = false;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.add_reusable_block(micro_ckpt_blk_idx_arr.at(i)));
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(micro_ckpt_blk_idx_arr.at(i), succ_free));
    ASSERT_EQ(true, succ_free);
  }
  ASSERT_EQ(0, blk_cnt_info.meta_blk_.used_cnt_);
  ASSERT_EQ(0, cache_stat.phy_blk_stat().meta_blk_used_cnt_);
  ASSERT_EQ(blk_cnt_info.meta_blk_.min_cnt_, blk_cnt_info.meta_blk_.hold_cnt_);
  ASSERT_EQ(blk_cnt_info.meta_blk_.min_cnt_, cache_stat.phy_blk_stat().meta_blk_cnt_);

  // 7. allocate all reorgan_block
  const int64_t reorgan_blk_cnt = blk_cnt_info.data_blk_.max_cnt_;
  block_idx = -1;
  ObArray<int64_t> reorgan_blk_idx_arr;
  for (int64_t i = 0; OB_SUCC(ret) && i < reorgan_blk_cnt; i++) {
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

  shared_blk_used_cnt = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;
  ASSERT_EQ(shared_blk_used_cnt, blk_cnt_info.shared_blk_used_cnt_);
  ASSERT_EQ(shared_blk_used_cnt, cache_stat.phy_blk_stat().shared_blk_used_cnt_);
  {
    // no free reorgan_block to allocate.
    ASSERT_EQ(OB_EAGAIN, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_REORGAN_BLK));
  }

  // 8. free all reorgan_block
  succ_free = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < reorgan_blk_cnt; i++) {
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

TEST_F(TestSSPhysicalBlockManager, reusable_set)
{
  int ret = OB_SUCCESS;
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ASSERT_EQ(true, phy_blk_mgr.is_inited_);

  ObSSPhysicalBlockHandle handle;
  int64_t block_idx = -1;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
  ASSERT_GT(block_idx, 1);
  bool is_exist = false;
  handle.get_ptr()->set_valid_len(256);
  hash::ObHashSet<int64_t>::iterator iter = phy_blk_mgr.reusable_set_.begin();
  for (; iter != phy_blk_mgr.reusable_set_.end() && !is_exist; iter++) {
    if (block_idx == iter->first) {
      is_exist = true;
    }
  }
  ASSERT_EQ(false, is_exist);

  bool is_empty = false;
  ASSERT_EQ(OB_SUCCESS, handle()->dec_valid_len(block_idx, -256));
  ASSERT_EQ(true, handle()->is_empty());
  is_exist = false;
  iter = phy_blk_mgr.reusable_set_.begin();
  for (; iter != phy_blk_mgr.reusable_set_.end() && !is_exist; iter++) {
    if (block_idx == iter->first) {
      is_exist = true;
    }
  }
  ASSERT_EQ(true, is_exist);

  handle.reset();
  bool succ_free = false;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.free_block(block_idx, succ_free));
  ASSERT_EQ(true, succ_free);
}

TEST_F(TestSSPhysicalBlockManager, resize_file_size)
{
  int ret = OB_SUCCESS;
  const static uint32_t BLOCK_SIZE = (1 << 21);

  // 1. total_block count is less than count of a sub_arr
  int64_t total_block_cnt = ObSSPhysicalBlockManager::PHY_BLOCK_SUB_ARR_SIZE / sizeof(ObSSPhysicalBlock) - 1;
  int64_t file_size = BLOCK_SIZE * total_block_cnt;

  ObSSMicroCacheStat cache_stat;
  ObSSPhysicalBlockManager blk_mgr(cache_stat);
  ASSERT_EQ(OB_SUCCESS, blk_mgr.init(MTL_ID(), file_size, BLOCK_SIZE));
  SSPhyBlockCntInfo &blk_cnt_info = blk_mgr.blk_cnt_info_;
  int64_t max_data_cnt = blk_cnt_info.cache_limit_blk_cnt();
  int64_t max_phy_ckpt_cnt = blk_cnt_info.phy_ckpt_blk_cnt_;

  ObArray<int64_t> phy_blk_idxs;
  ObArray<ObSSPhysicalBlock *> phy_blocks;
  for (int64_t i = 0; i < max_data_cnt; i++) {
    int64_t block_idx = -1;
    ObSSPhysicalBlockHandle phy_block_handle;
    ASSERT_EQ(OB_SUCCESS, blk_mgr.alloc_block(block_idx, phy_block_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
    ASSERT_EQ(true, phy_block_handle.is_valid());
    phy_block_handle.get_ptr()->is_sealed_ = true;
    ASSERT_EQ(OB_SUCCESS, phy_blocks.push_back(phy_block_handle.get_ptr()));
    ASSERT_EQ(OB_SUCCESS, phy_blk_idxs.push_back(block_idx));
  }
  for (int64_t i = 0; i < max_phy_ckpt_cnt; ++i) {
    int64_t block_idx = -1;
    ObSSPhysicalBlockHandle phy_block_handle;
    ASSERT_EQ(OB_SUCCESS, blk_mgr.alloc_block(block_idx, phy_block_handle, ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK));
    ASSERT_EQ(true, phy_block_handle.is_valid());
    phy_block_handle.get_ptr()->is_sealed_ = true;
    ASSERT_EQ(OB_SUCCESS, phy_blocks.push_back(phy_block_handle.get_ptr()));
    ASSERT_EQ(OB_SUCCESS, phy_blk_idxs.push_back(block_idx));
  }

  // 2. resize file_size
  const int64_t old_min_data_blk_cnt = blk_cnt_info.data_blk_.min_cnt_;
  const int64_t old_max_data_blk_cnt = blk_cnt_info.data_blk_.max_cnt_;
  const int64_t old_used_data_blk_cnt = blk_cnt_info.data_blk_.used_cnt_;
  const int64_t old_min_micro_blk_cnt = blk_cnt_info.meta_blk_.min_cnt_;
  const int64_t old_max_micro_blk_cnt = blk_cnt_info.meta_blk_.max_cnt_;
  const int64_t old_micro_blk_cnt = blk_cnt_info.meta_blk_.used_cnt_;
  ASSERT_EQ(OB_SUCCESS, blk_mgr.format_ss_super_block());
  total_block_cnt += ObSSPhysicalBlockManager::PHY_BLOCK_SUB_ARR_SIZE / sizeof(ObSSPhysicalBlock);
  file_size = BLOCK_SIZE * total_block_cnt;
  ASSERT_EQ(OB_SUCCESS, blk_mgr.resize_file_size(file_size, BLOCK_SIZE));
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

  // alloc more cache_data block after resize
  int64_t new_max_data_cnt = blk_cnt_info.cache_limit_blk_cnt();
  ASSERT_LT(max_data_cnt, new_max_data_cnt);
  int64_t new_max_phy_ckpt_cnt = blk_cnt_info.phy_ckpt_blk_cnt_;
  ASSERT_LE(max_phy_ckpt_cnt, new_max_phy_ckpt_cnt);
  for (int64_t i = max_data_cnt; i < new_max_data_cnt; i++) {
    int64_t block_idx = -1;
    ObSSPhysicalBlockHandle phy_block_handle;
    ASSERT_EQ(OB_SUCCESS, blk_mgr.alloc_block(block_idx, phy_block_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
    ASSERT_EQ(true, phy_block_handle.is_valid());
    phy_block_handle.get_ptr()->is_sealed_ = true;
    ASSERT_EQ(OB_SUCCESS, phy_blocks.push_back(phy_block_handle.get_ptr()));
    ASSERT_EQ(OB_SUCCESS, phy_blk_idxs.push_back(block_idx));
  }
  for (int64_t i = max_phy_ckpt_cnt; i < new_max_phy_ckpt_cnt; ++i) {
    int64_t block_idx = -1;
    ObSSPhysicalBlockHandle phy_block_handle;
    ASSERT_EQ(OB_SUCCESS, blk_mgr.alloc_block(block_idx, phy_block_handle, ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK));
    ASSERT_EQ(true, phy_block_handle.is_valid());
    phy_block_handle.get_ptr()->is_sealed_ = true;
    ASSERT_EQ(OB_SUCCESS, phy_blocks.push_back(phy_block_handle.get_ptr()));
    ASSERT_EQ(OB_SUCCESS, phy_blk_idxs.push_back(block_idx));
  }

  // 3. check the original phy_block's ptr not changed
  for (int64_t i = 0; OB_SUCC(ret) && i < max_data_cnt + max_phy_ckpt_cnt; i++) {
    const int64_t block_idx = phy_blk_idxs.at(i);
    ObSSPhysicalBlockHandle handle;
    ASSERT_EQ(OB_SUCCESS, blk_mgr.get_block_handle(block_idx, handle));
    ASSERT_EQ(true, handle.is_valid());
    ASSERT_EQ(phy_blocks.at(i), handle.get_ptr());
  }

  // 4. check handle
  ObSSPhysicalBlockHandle handle1;
  handle1.set_ptr(phy_blocks.at(0));
  ASSERT_EQ(2, handle1.ptr_->ref_cnt_);
  ObSSPhysicalBlockHandle handle2;
  ASSERT_EQ(OB_SUCCESS, handle2.assign(handle1));
  ASSERT_EQ(3, handle2.ptr_->ref_cnt_);
  handle2.reset();
  handle1.reset();

  // 5. free all allocated phy_blocks
  bool succ_free = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < phy_blk_idxs.count(); i++) {
    succ_free = false;
    const int64_t block_idx = phy_blk_idxs.at(i);
    ASSERT_EQ(OB_SUCCESS, blk_mgr.add_reusable_block(block_idx));
    ASSERT_EQ(OB_SUCCESS, blk_mgr.free_block(block_idx, succ_free));
    ASSERT_EQ(true, succ_free);
  }
  ASSERT_EQ(0, blk_cnt_info.data_blk_.used_cnt_);
  ASSERT_EQ(0, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_EQ(0, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(0, cache_stat.phy_blk_stat().phy_ckpt_blk_used_cnt_);
}

/* Multiple threads allocate/free blocks in parallel. */
TEST_F(TestSSPhysicalBlockManager, test_parallel_allocate_block)
{
  int ret = OB_SUCCESS;
  const static uint64_t FILE_SIZE = (1L << 32);
  const static uint32_t BLOCK_SIZE = (1 << 21);
  ObSSMicroCacheStat cache_stat;
  ObSSPhysicalBlockManager blk_mgr(cache_stat);
  const uint64_t tenant_id = MTL_ID();
  ASSERT_EQ(true, is_valid_tenant_id(tenant_id));
  ASSERT_EQ(OB_SUCCESS, blk_mgr.init(tenant_id, FILE_SIZE, BLOCK_SIZE));
  SSPhyBlockCntInfo &blk_cnt_info = blk_mgr.blk_cnt_info_;

  const int64_t max_reorgan_blk_cnt = blk_cnt_info.reorgan_blk_cnt_ * 2;
  const int64_t max_data_blk_cnt = blk_cnt_info.shared_blk_cnt_ * (MIN_CACHE_DATA_BLOCK_CNT_PCT + 5) / 100 - max_reorgan_blk_cnt;    // 85%
  const int64_t max_micro_ckpt_cnt = blk_cnt_info.shared_blk_cnt_ * (MIN_MICRO_CKPT_BLOCK_CNT_PCT + 5) / 100;  // 9%
  const int64_t data_blk_thread_num = 5;
  const int64_t micro_ckpt_blk_thread_num = 5;
  const int64_t phy_ckpt_blk_thread_num = 1;
  const int64_t reorgan_blk_thread_num = 1;

  int64_t data_blk_cnt = max_data_blk_cnt / data_blk_thread_num;
  int64_t micro_ckpt_cnt = max_micro_ckpt_cnt / micro_ckpt_blk_thread_num;
  int64_t phy_ckpt_cnt = blk_cnt_info.phy_ckpt_blk_cnt_ / phy_ckpt_blk_thread_num;
  int64_t reorgan_blk_cnt = max_reorgan_blk_cnt / reorgan_blk_thread_num;
  TestSSPhysicalBlockManager::TestSSPhyBlockMgrThread threads(ObTenantEnv::get_tenant(),
      &blk_mgr,
      data_blk_cnt,
      phy_ckpt_cnt,
      micro_ckpt_cnt,
      max_reorgan_blk_cnt,
      TestSSPhyBlockMgrThread::TestParallelType::TEST_PARALLEL_ALLOCATE_BLOCK);
  threads.set_thread_count(12);
  threads.start();
  threads.wait();
  ASSERT_EQ(0, threads.get_fail_cnt());

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

/* Multiple threads allocate/free normal blocks in parallel for multiple rounds and check reuse version. */
TEST_F(TestSSPhysicalBlockManager, test_parallel_allocate_block_and_check_reuse_version)
{
  int ret = OB_SUCCESS;
  const static uint64_t FILE_SIZE = (1L << 40);
  const static uint32_t BLOCK_SIZE = (1 << 21);
  ObSSMicroCacheStat cache_stat;
  ObSSPhysicalBlockManager blk_mgr(cache_stat);
  const uint64_t tenant_id = MTL_ID();
  ASSERT_EQ(true, is_valid_tenant_id(tenant_id));
  ASSERT_EQ(OB_SUCCESS, blk_mgr.init(tenant_id, FILE_SIZE, BLOCK_SIZE));

  const int32_t thread_cnt = 2;
  int64_t data_blk_cnt = blk_mgr.blk_cnt_info_.cache_limit_blk_cnt() / thread_cnt;
  data_blk_cnt = MIN(2000, data_blk_cnt);
  int64_t phy_ckpt_blk_cnt = 0;
  int64_t micro_ckpt_blk_cnt = 0;
  int64_t reorgan_blk_cnt = 0;
  int64_t ori_total_reuse_version = 0;
  for (int64_t i = 2; OB_SUCC(ret) && i < blk_mgr.blk_cnt_info_.total_blk_cnt_; i++) {
    ObSSPhysicalBlockHandle handle;
    ASSERT_EQ(OB_SUCCESS, blk_mgr.get_block_handle(i, handle));
    handle.get_ptr()->reuse_version_ = 1;
    ++ori_total_reuse_version;
  }

  TestSSPhysicalBlockManager::TestSSPhyBlockMgrThread threads(ObTenantEnv::get_tenant(),
      &blk_mgr,
      data_blk_cnt,
      phy_ckpt_blk_cnt,
      micro_ckpt_blk_cnt,
      reorgan_blk_cnt,
      TestSSPhyBlockMgrThread::TestParallelType::TEST_PARALLEL_ALLOCATE_BLOCK_AND_CHECK_REUSE_VERSION);
  threads.set_thread_count(thread_cnt);
  threads.start();
  threads.wait();

  int64_t block_idx = -1;
  const int64_t data_blk_used_cnt = blk_mgr.blk_cnt_info_.data_blk_.used_cnt_;
  ASSERT_EQ(0, data_blk_used_cnt);
  int64_t total_reuse_version = 0;
  for (int64_t i = 2; OB_SUCC(ret) && i < blk_mgr.blk_cnt_info_.total_blk_cnt_; i++) {
    ObSSPhysicalBlockHandle handle;
    ASSERT_EQ(OB_SUCCESS, blk_mgr.get_block_handle(i, handle));
    total_reuse_version += handle.get_ptr()->reuse_version_;
  }
  ASSERT_EQ(0, threads.get_fail_cnt());
  ASSERT_EQ(data_blk_cnt * 10 * thread_cnt, total_reuse_version - ori_total_reuse_version);
}

/* Multiple threads allocate cache_data_blocks and resize file size in parallel. */
TEST_F(TestSSPhysicalBlockManager, test_parallel_allocate_block_and_resize)
{
  int ret = OB_SUCCESS;
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  const int64_t ori_file_size = phy_blk_mgr.total_file_size_;

  const int64_t thread_cnt = 10;
  int64_t data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt() / thread_cnt;
  data_blk_cnt = MIN(2000, data_blk_cnt);
  int64_t phy_ckpt_blk_cnt = 0;
  int64_t micro_ckpt_blk_cnt = 0;
  int64_t reorgan_blk_cnt = 0;
  TestSSPhysicalBlockManager::TestSSPhyBlockMgrThread threads(ObTenantEnv::get_tenant(),
      &phy_blk_mgr,
      data_blk_cnt,
      phy_ckpt_blk_cnt,
      micro_ckpt_blk_cnt,
      reorgan_blk_cnt,
      TestSSPhyBlockMgrThread::TestParallelType::TEST_PARALLEL_ALLOCATE_BLOCK_AND_RESIZE);
  threads.set_thread_count(thread_cnt);
  threads.start();
  threads.wait();

  ASSERT_EQ(data_blk_cnt * 5, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
  ASSERT_EQ(ori_file_size * 5, phy_blk_mgr.total_file_size_);
  ASSERT_EQ(0, threads.get_fail_cnt());
}

/*
  Test two scenarios:
    1: Both super_blocks are correct, read the latest one
    2: One of the super_blocks is correct and the other is incorrect, read the correct one
*/
TEST_F(TestSSPhysicalBlockManager, test_double_write_super_blk)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObMemAttr attr(OB_SERVER_TENANT_ID, "test");
  const int64_t align_size = SS_MEM_BUF_ALIGNMENT;
  char *buf = static_cast<char *>(ob_malloc_align(SS_MEM_BUF_ALIGNMENT, align_size, attr));
  ASSERT_NE(nullptr, buf);

  // Scenario 1
  const int64_t old_cache_file_size = phy_blk_mgr.total_file_size_ * 2;
  const int64_t new_cache_file_size = phy_blk_mgr.total_file_size_ * 4;
  ObSSMicroCacheSuperBlock old_super_blk(old_cache_file_size);
  ObSSMicroCacheSuperBlock new_super_blk(new_cache_file_size);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_ss_super_block(old_super_blk));

  new_super_blk.modify_time_us_ = old_super_blk.modify_time_us_ + 888888;
  ASSERT_EQ(OB_SUCCESS, serialize_super_block(buf, align_size, new_super_blk));
  int64_t write_size = 0;
  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->pwrite_cache_block(0, align_size, buf, write_size));

  ObSSMicroCacheSuperBlock super_blk;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.read_ss_super_block(super_blk));
  check_super_block_identical(phy_blk_mgr.super_block_, new_super_blk);

  // Scenario 2
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.update_ss_super_block(new_super_blk));

  const int64_t fake_cache_file_size = phy_blk_mgr.total_file_size_ * 8;
  ObSSMicroCacheSuperBlock fake_super_blk(new_cache_file_size);
  fake_super_blk.modify_time_us_ = new_super_blk.modify_time_us_ + 888888;
  ASSERT_EQ(OB_SUCCESS, serialize_super_block(buf, align_size, fake_super_blk));
  buf[fake_super_blk.get_serialize_size() - 3] = '#'; // simulate data error
  ObSSMicroCacheSuperBlock tmp_super_block;
  ASSERT_NE(OB_SUCCESS, deserialize_super_block(buf, align_size, tmp_super_block));

  write_size = 0;
  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->pwrite_cache_block(0, align_size, buf, write_size));
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.read_ss_super_block(super_blk));
  check_super_block_identical(phy_blk_mgr.super_block_, new_super_blk);

  ob_free_align(buf);
}

TEST_F(TestSSPhysicalBlockManager, test_scan_reorgan_blk)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  const int64_t block_size = phy_blk_mgr.get_block_size();
  int64_t total_data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
  ASSERT_LT(10, total_data_blk_cnt);
  int64_t block_idx = -1;
  ObSSPhysicalBlockHandle phy_blk_handle;
  ObArray<int64_t> normal_blk_idx_arr;
  for (int64_t i = 0; OB_SUCC(ret) && i < total_data_blk_cnt; i++) {
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
    ASSERT_EQ(OB_SUCCESS, normal_blk_idx_arr.push_back(block_idx));
    ASSERT_EQ(true, phy_blk_handle.is_valid());

    phy_blk_handle()->is_sealed_ = true;
    phy_blk_handle()->is_free_ = false;
    phy_blk_handle()->block_type_ = static_cast<int64_t>(ObSSPhyBlockType::SS_CACHE_DATA_BLK);
    phy_blk_handle()->valid_len_ = phy_blk_mgr.get_block_size();
    ASSERT_EQ(false, phy_blk_handle()->can_reorganize(block_size));
  }

  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.scan_sparse_blocks());
  ASSERT_EQ(0, phy_blk_mgr.get_sparse_block_cnt());

  const int64_t dec_len = -1 * phy_blk_mgr.get_block_size() * 0.2;
  for (int64_t i = 0; i < 10; i++) {
    const int64_t block_idx = normal_blk_idx_arr[i];
    ObSSPhysicalBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(block_idx, phy_blk_handle));
    ASSERT_EQ(OB_SUCCESS, phy_blk_handle()->dec_valid_len(block_idx, dec_len));
  }
  ASSERT_EQ(10, phy_blk_mgr.get_sparse_block_cnt());

  for (int64_t i = 0; i < 10; ++i) {
    const int64_t block_idx = normal_blk_idx_arr[i];
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.delete_sparse_block(block_idx));
  }
  ASSERT_EQ(0, phy_blk_mgr.get_sparse_block_cnt());

  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.scan_sparse_blocks());
  ASSERT_EQ(10, phy_blk_mgr.get_sparse_block_cnt());
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_physical_block_manager.log*");
  OB_LOGGER.set_file_name("test_ss_physical_block_manager.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}