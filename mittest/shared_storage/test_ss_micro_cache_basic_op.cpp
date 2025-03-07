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

class TestSSMicroCacheBasicOp : public ::testing::Test
{
public:
  TestSSMicroCacheBasicOp() : mem_blk_(nullptr), micro_meta_(nullptr) {}
  virtual ~TestSSMicroCacheBasicOp(){};
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  int alloc_and_init_micro_meta(ObSSMicroBlockMeta *&micro_meta, const ObSSMemBlockHandle &mem_blk_handle);
  int mock_micro_block_persisted(ObSSMicroBlockMeta *micro_meta);

private:
  const static uint32_t ORI_MICRO_REF_CNT = 0;

private:
  ObSSMemBlock *mem_blk_;
  ObSSMicroBlockMeta *micro_meta_;
};

void TestSSMicroCacheBasicOp::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheBasicOp::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheBasicOp::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 31)));
  micro_cache->start();
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSMicroCache *)->mem_data_mgr_.do_alloc_mem_block(mem_blk_, true));
  ASSERT_EQ(true, mem_blk_->is_valid());
  ObSSMemBlockHandle mem_handle;
  mem_handle.set_ptr(mem_blk_);
  ASSERT_EQ(OB_SUCCESS, alloc_and_init_micro_meta(micro_meta_, mem_handle));
  ASSERT_EQ(true, micro_meta_->is_valid());
}

void TestSSMicroCacheBasicOp::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  mem_blk_->destroy();
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

int TestSSMicroCacheBasicOp::alloc_and_init_micro_meta(
  ObSSMicroBlockMeta *&micro_meta,
  const ObSSMemBlockHandle &mem_blk_handle)
{
  int ret = OB_SUCCESS;
  const MacroBlockId macro_id(0, 99, 0);
  const int64_t offset = 100;
  const int64_t size = 100;
  const uint32_t crc = 0;
  ObSSMicroBlockMeta *tmp_micro_meta = nullptr;
  ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, size);
  if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(mem_blk_handle));
  } else if (OB_UNLIKELY(!mem_blk_handle()->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem_blk is invalid", KR(ret), KPC(mem_blk_handle.get_ptr()));
  } else if (OB_FAIL(TestSSCommonUtil::alloc_micro_block_meta(tmp_micro_meta))) {
    LOG_WARN("fail to alloc micro_meta", KR(ret));
  } else if (OB_ISNULL(tmp_micro_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_meta should not be null", KR(ret), KP(tmp_micro_meta));
  } else if (OB_FALSE_IT(tmp_micro_meta->reset())) {
  } else if (OB_FAIL(tmp_micro_meta->init(micro_key, mem_blk_handle, size, crc))) {
    LOG_WARN("fail to init micro_meta", KR(ret), K(micro_key), KPC(mem_blk_handle.get_ptr()));
  } else {
    ObSSMemMicroInfo mem_micro_info(offset, size);
    if (OB_FAIL(mem_blk_handle()->micro_offset_map_.set_refactored(micro_key, mem_micro_info, 1 /*overwrite*/))) {
      LOG_WARN("fail to set refactored", KR(ret), K(micro_key), K(mem_micro_info));
    } else {
      mem_blk_handle()->add_valid_micro_block(size);
      micro_meta = tmp_micro_meta;
    }
  }
  return ret;
}

int TestSSMicroCacheBasicOp::mock_micro_block_persisted(ObSSMicroBlockMeta *micro_meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(micro_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(micro_meta));
  } else if (OB_UNLIKELY(!micro_meta->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("micro_meta is invalid", KR(ret), KPC(micro_meta));
  } else {
    int64_t block_idx = 0;
    ObSSPhysicalBlockHandle phy_handle;
    if (OB_FAIL(SSPhyBlockMgr.alloc_block(block_idx, phy_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK))) {
      LOG_WARN("fail to alloc phy_block", KR(ret));
    } else if (OB_UNLIKELY(!phy_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_handle is invalid", KR(ret), K(block_idx), K(phy_handle));
    } else {
      micro_meta->is_persisted_ = true;
      micro_meta->reuse_version_ = phy_handle.get_ptr()->reuse_version_;
      micro_meta->data_dest_ = block_idx * SSPhyBlockMgr.get_block_size() + 100;
    }
  }
  return ret;
}

TEST_F(TestSSMicroCacheBasicOp, get_micro_handle_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();
  int64_t reuse_version = micro_meta_->reuse_version_;
  int64_t data_dest = micro_meta_->data_dest_;

  // 1. micro_block is in mem_block, and is transferred to T2 after ‘get’
  ObSSMemBlockHandle mem_handle;
  ObSSPhysicalBlockHandle phy_handle;
  ObSSMicroBlockMetaHandle micro_handle;
  ASSERT_EQ(true, micro_meta_->is_in_l1_);
  SSMicroMapGetMicroHandleFunc func1(micro_handle, mem_handle, phy_handle, true);
  ret = micro_map.operate(&micro_key, func1);
  ret = ((OB_EAGAIN == ret) ? func1.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(mem_handle.get_ptr(), mem_blk_);
  ASSERT_EQ(false, micro_meta_->is_in_l1_); // transfer to T2

  // 2. mem_block reuse_version dismatch, return OB_ENTRY_NOT_EXIST
  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  micro_meta_->reuse_version_ = 100;
  SSMicroMapGetMicroHandleFunc func2(micro_handle, mem_handle, phy_handle, true);
  ret = micro_map.operate(&micro_key, func2);
  ret = ((OB_EAGAIN == ret) ? func2.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_EQ(false, micro_meta_->is_valid());

  // 3. micro_meta is marked invalid, return OB_ENTRY_NOT_EXIST
  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  SSMicroMapGetMicroHandleFunc func3(micro_handle, mem_handle, phy_handle, true);
  ret = micro_map.operate(&micro_key, func3);
  ret = ((OB_EAGAIN == ret) ? func3.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // 4. micro_block is persisted
  micro_meta_->reuse_version_ = reuse_version;
  micro_meta_->data_dest_ = data_dest;
  ASSERT_EQ(OB_SUCCESS, mock_micro_block_persisted(micro_meta_));
  ASSERT_EQ(true, micro_meta_->is_valid());
  reuse_version = micro_meta_->reuse_version_;
  data_dest = micro_meta_->data_dest_;

  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  SSMicroMapGetMicroHandleFunc func4(micro_handle, mem_handle, phy_handle, true);
  ret = micro_map.operate(&micro_key, func4);
  ret = ((OB_EAGAIN == ret) ? func4.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. phy_block reuse_version dismatch, return OB_ENTRY_NOT_EXIST
  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  micro_meta_->reuse_version_ = 100;
  SSMicroMapGetMicroHandleFunc func5(micro_handle, mem_handle, phy_handle, true);
  ret = micro_map.operate(&micro_key, func5);
  ret = ((OB_EAGAIN == ret) ? func5.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // 6. set update_arc=true, it will be transfered to T2
  bool update_arc = true;
  micro_meta_->reuse_version_ = reuse_version;
  micro_meta_->data_dest_ = data_dest;
  micro_meta_->is_in_l1_ = true;
  micro_meta_->is_in_ghost_ = false;
  ASSERT_EQ(true, micro_meta_->is_valid());
  ASSERT_EQ(true, micro_meta_->is_persisted_);
  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  SSMicroMapGetMicroHandleFunc func6(micro_handle, mem_handle, phy_handle, update_arc);
  ret = micro_map.operate(&micro_key, func6);
  ret = ((OB_EAGAIN == ret) ? func6.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, micro_meta_->is_valid());
  ASSERT_EQ(false, micro_meta_->is_in_l1_);
  ASSERT_EQ(false, micro_meta_->is_in_ghost_);
  ASSERT_EQ(true, micro_meta_->is_persisted_);

  // 7. set update_arc=false, it won't transfer seg.
  update_arc = false;
  micro_meta_->reuse_version_ = reuse_version;
  micro_meta_->data_dest_ = data_dest;
  micro_meta_->is_persisted_ = true;
  micro_meta_->is_in_l1_ = true;
  micro_meta_->is_in_ghost_ = false;
  ASSERT_EQ(true, micro_meta_->is_valid());
  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  SSMicroMapGetMicroHandleFunc func7(micro_handle, mem_handle, phy_handle, update_arc);
  ret = micro_map.operate(&micro_key, func7);
  ret = ((OB_EAGAIN == ret) ? func7.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, micro_meta_->is_valid());
  ASSERT_EQ(true, micro_meta_->is_in_l1_);
  ASSERT_EQ(false, micro_meta_->is_in_ghost_);
  ASSERT_EQ(true, micro_meta_->is_persisted_);
}

TEST_F(TestSSMicroCacheBasicOp, update_micro_heat_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();
  uint64_t old_heat_val = micro_meta_->get_heat_val();

  const bool transfer_seg = true;
  const bool update_access_time = true;
  int64_t time_delta_s = 100;
  // 1. if hit T1, transfer seg and update heat
  SSMicroMapUpdateMicroHeatFunc func1(transfer_seg, update_access_time, time_delta_s);
  ret = micro_map.operate(&micro_key, func1);
  ret = ((OB_EAGAIN == ret) ? func1.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObSSARCOpType::SS_ARC_HIT_T1, func1.arc_op_type_);
  ASSERT_EQ(false, micro_meta_->is_in_l1_);
  ASSERT_GT(micro_meta_->get_heat_val(), old_heat_val);
  old_heat_val = micro_meta_->get_heat_val();

  // 2. if hit ghost, don't transfer seg, just update heat
  micro_meta_->is_in_ghost_ = true;
  time_delta_s = 200;
  SSMicroMapUpdateMicroHeatFunc func2(transfer_seg, update_access_time, time_delta_s);
  ret = micro_map.operate(&micro_key, func2);
  ret = ((OB_EAGAIN == ret) ? func2.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObSSARCOpType::SS_ARC_INVALID_OP, func2.arc_op_type_);
  ASSERT_GT(micro_meta_->get_heat_val(), old_heat_val);
}

TEST_F(TestSSMicroCacheBasicOp, deleted_unpersisted_micro_func)
{
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  ObSSMicroMetaManager::SSMicroMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta_->ref_cnt_);

  ObSSMicroBlockMetaHandle micro_handle;
  micro_handle.set_ptr(micro_meta_);
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta_->ref_cnt_);

  // 1. fail to delete micro_block which is persisted
  ObSSMemBlockHandle mem_handle;
  mem_handle.set_ptr(mem_blk_);
  micro_meta_->is_persisted_ = true;
  SSMicroMapDeleteUnpersistedMicroFunc func1(mem_handle);
  ASSERT_EQ(OB_EAGAIN, micro_map.erase_if(&micro_key, func1));
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta_->ref_cnt_);

  // 2. succeed to delete micro_block which is not persisted
  micro_meta_->is_persisted_ = false;
  SSMicroMapDeleteUnpersistedMicroFunc func2(mem_handle);
  ASSERT_EQ(OB_SUCCESS, micro_map.erase_if(&micro_key, func2));
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta_->ref_cnt_);
  ASSERT_EQ(micro_meta_->length_, func2.size_);
  ASSERT_EQ(micro_meta_->is_in_l1_, func2.is_in_l1_);
  ASSERT_EQ(micro_meta_->is_in_ghost_, func2.is_in_ghost_);

  micro_handle.reset();
  ASSERT_EQ(0, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
}

TEST_F(TestSSMicroCacheBasicOp, deleted_invalid_micro_func)
{
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  ObSSMicroMetaManager::SSMicroMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();

  ObSSMicroBlockMetaHandle micro_handle;
  micro_handle.set_ptr(micro_meta_);
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta_->ref_cnt_);

  // 1. fail to delete valid micro_meta
  SSMicroMapDeleteInvalidMicroFunc func1;
  ASSERT_EQ(OB_EAGAIN, micro_map.erase_if(&micro_key, func1));

  // 2. succeed to delete invalid micro_meta
  micro_meta_->mark_invalid();
  SSMicroMapDeleteInvalidMicroFunc func2;
  ASSERT_EQ(OB_SUCCESS, micro_map.erase_if(&micro_key, func2));
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta_->ref_cnt_);
  ASSERT_EQ(micro_meta_->length_, func2.size_);
  ASSERT_EQ(micro_meta_->is_in_l1_, func2.is_in_l1_);
  ASSERT_EQ(micro_meta_->is_in_ghost_, func2.is_in_ghost_);

  micro_handle.reset();
  ASSERT_EQ(0, SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
}

TEST_F(TestSSMicroCacheBasicOp, invalidate_micro_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();

  ASSERT_EQ(true, micro_meta_->is_valid_field());
  ASSERT_EQ(true, micro_meta_->is_in_l1_);
  ASSERT_EQ(false, micro_meta_->is_in_ghost_);

  micro_meta_->is_reorganizing_ = true;
  SSMicroMapInvalidateMicroFunc invalidate_func;
  ret = micro_map.operate(&micro_key, invalidate_func);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ((OB_EAGAIN == ret) ? invalidate_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, invalidate_func.succ_invalidate_);

  micro_meta_->is_reorganizing_ = false;
  SSMicroMapInvalidateMicroFunc invalidate_func1;
  ret = micro_map.operate(&micro_key, invalidate_func1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ((OB_EAGAIN == ret) ? invalidate_func1.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, invalidate_func1.succ_invalidate_);
  ASSERT_EQ(false, micro_meta_->is_valid_field());
  ASSERT_EQ(true, micro_meta_->is_in_l1_);
  ASSERT_EQ(true, micro_meta_->is_in_ghost_);

  SSMicroMapInvalidateMicroFunc invalidate_func2;
  ret = micro_map.operate(&micro_key, invalidate_func2);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ((OB_EAGAIN == ret) ? invalidate_func2.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestSSMicroCacheBasicOp, try_reorganize_micro_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();

  // mock micro_block is persisted
  ASSERT_EQ(OB_SUCCESS, mock_micro_block_persisted(micro_meta_));
  ASSERT_EQ(true, micro_meta_->is_valid());

  // 1. fail to set is_reorganizing if micro_meta is in ghost
  micro_meta_->is_in_ghost_ = true;
  ObSSMicroBlockMetaHandle micro_handle;
  SSMicroMapTryReorganizeMicroFunc func1(micro_meta_->data_dest_, micro_handle);
  ret = micro_map.operate(&micro_key, func1);
  ret = ((OB_EAGAIN == ret) ? func1.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_EQ(false, micro_meta_->is_reorganizing_);
  ASSERT_EQ(false, micro_handle.is_valid());

  // 2. fail to set is_reorganizing if micro_meta mismatch phy_block's reuse_version
  micro_handle.reset();
  micro_meta_->is_in_ghost_ = false;
  const int64_t reuse_version = micro_meta_->reuse_version_;
  micro_meta_->reuse_version_ = 10;
  SSMicroMapTryReorganizeMicroFunc func2(micro_meta_->data_dest_, micro_handle);
  ret = micro_map.operate(&micro_key, func2);
  ret = ((OB_EAGAIN == ret) ? func2.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_EQ(false, micro_meta_->is_reorganizing_);
  ASSERT_EQ(false, micro_handle.is_valid());

  // 3. succeed to set micro_meta is_reorganizing
  micro_handle.reset();
  micro_meta_->reuse_version_ = reuse_version;
  SSMicroMapTryReorganizeMicroFunc func3(micro_meta_->data_dest_, micro_handle);
  ret = micro_map.operate(&micro_key, func3);
  ret = ((OB_EAGAIN == ret) ? func3.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, micro_meta_->is_reorganizing_);
  ASSERT_EQ(true, micro_handle.is_valid());
}

TEST_F(TestSSMicroCacheBasicOp, complete_reorganizing_micro_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();

  const int64_t old_valid_val = mem_blk_->valid_val_;
  ASSERT_EQ(OB_SUCCESS, mock_micro_block_persisted(micro_meta_));
  ASSERT_EQ(true, micro_meta_->is_valid());
  // mock micro_block is reorganizing
  micro_meta_->is_reorganizing_ = true;

  ObSSMemBlockHandle mem_handle;
  mem_handle.set_ptr(mem_blk_);
  SSMicroMapCompleteReorganizingMicroFunc func1(mem_handle);
  ret = micro_map.operate(&micro_key, func1);
  ret = ((OB_EAGAIN == ret) ? func1.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, micro_meta_->is_reorganizing_);
  ASSERT_EQ(false, micro_meta_->is_persisted_);
  ASSERT_EQ(mem_blk_->reuse_version_, micro_meta_->reuse_version_);
  ASSERT_EQ(mem_blk_, reinterpret_cast<ObSSMemBlock *>(micro_meta_->data_dest_));
  ASSERT_EQ(old_valid_val + micro_meta_->length_, mem_blk_->valid_val_);
}

TEST_F(TestSSMicroCacheBasicOp, unmark_reorganize_micro_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();

  micro_meta_->is_reorganizing_ = true;
  SSMicroMapUnmarkReorganizingMicroFunc func;
  ret = micro_map.operate(&micro_key, func);
  ret = ((OB_EAGAIN == ret) ? func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, micro_meta_->is_reorganizing_);
}

TEST_F(TestSSMicroCacheBasicOp, micro_map_func)
{
  int ret = OB_SUCCESS;
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  ObArenaAllocator allocator;
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSMicroMetaManager::SSMicroMap &micro_map = micro_meta_mgr.micro_meta_map_;
  ObSSMicroBlockMetaHandle ori_micro_meta_handle;
  ori_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&(micro_meta_->micro_key()), ori_micro_meta_handle));
  ori_micro_meta_handle.reset();

  MacroBlockId macro_id(0, 100, 0);
  int64_t offset = 1;
  int64_t size = 256;
  ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, size);
  char *micro_data = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, micro_data);
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::gen_random_data(micro_data, size));

  ObSSMemBlock *mem_blk_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSMicroCache *)->mem_data_mgr_.do_alloc_mem_block(mem_blk_ptr, true/*is_fg*/));
  ASSERT_NE(nullptr, mem_blk_ptr);
  ASSERT_EQ(true, mem_blk_ptr->is_valid());
  ObSSMemBlock &mem_blk = *mem_blk_ptr;

  int32_t data_offset = -1;
  int32_t idx_offset = -1;
  uint32_t crc = 0;
  ASSERT_EQ(OB_SUCCESS, mem_blk.calc_write_location(micro_key, size, data_offset, idx_offset));
  ASSERT_EQ(OB_SUCCESS, mem_blk.write_micro_data(micro_key, micro_data, size, data_offset, idx_offset, crc));
  ASSERT_EQ(0, mem_blk.valid_val_);

  // micro_block meta not exist
  ObSSMicroBlockMetaHandle micro_handle;
  ret = micro_map.get(&micro_key, micro_handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // create a new micro_block meta, it is in T1
  ObSSMemBlockHandle mem_blk_handle;
  mem_blk_handle.set_ptr(&mem_blk);
  bool succ_add_new = false;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.add_or_update_micro_block_meta(micro_key, size, crc, mem_blk_handle, succ_add_new));
  ASSERT_EQ(true, succ_add_new);

  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, false));
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_handle.get_ptr()->ref_cnt_);
  ObSSMicroBlockMeta *micro_meta = micro_handle.get_ptr();
  ASSERT_EQ(size, mem_blk.valid_val_);
  ASSERT_EQ(true, mem_blk.is_completed());
  mem_blk_handle.reset();

  // get micro_meta, already exist, and it will be in T2
  ObSSMicroBlockCacheKey tmp_micro_key = micro_key;
  ObSSMicroBlockMetaHandle tmp_micro_handle;
  ret = micro_map.get(&micro_key, tmp_micro_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, tmp_micro_handle.is_valid());
  tmp_micro_handle()->transfer_arc_seg(ObSSARCOpType::SS_ARC_HIT_T1);
  ASSERT_EQ(ori_micro_ref_cnt + 3, tmp_micro_handle.get_ptr()->ref_cnt_);
  ASSERT_EQ(true, tmp_micro_handle.get_ptr()->is_valid());
  ASSERT_EQ(micro_meta, tmp_micro_handle.get_ptr());
  ASSERT_EQ(false, micro_meta->is_in_l1_);
  ASSERT_EQ(false, micro_meta->is_in_ghost_);
  ASSERT_EQ(false, micro_meta->is_persisted_);
  tmp_micro_handle.reset();
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_handle.get_ptr()->ref_cnt_);

  // write the same micro_block data into mem_block again
  data_offset += size;
  idx_offset *= 2;
  ASSERT_EQ(OB_SUCCESS, mem_blk.write_micro_data(micro_key, micro_data, size, data_offset, idx_offset, crc));

  // this micro_block meta already exist, we try to update it, actually treat it as 'HIT_T2'
  mem_blk_handle.set_ptr(&mem_blk);
  SSMicroMapUpdateMicroFunc update_func(size, mem_blk_handle, crc);
  ret = micro_map.operate(&micro_key, update_func);
  ret = ((OB_EAGAIN == ret) ? update_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObSSARCOpType::SS_ARC_HIT_T2, update_func.arc_op_type_);
  ASSERT_EQ(false, micro_meta->is_in_l1_);
  ASSERT_EQ(false, micro_meta->is_in_ghost_);
  mem_blk_handle.reset();

  // get this micro_block meta location
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  ObSSPhysicalBlockHandle phy_blk_handle;
  SSMicroMapGetMicroHandleFunc get_handle_func(tmp_micro_handle, mem_blk_handle, phy_blk_handle);
  ObSSMicroBlockCacheKey invalid_micro_key = micro_key;
  invalid_micro_key.micro_id_.offset_ += 1;
  ret = micro_map.operate(&invalid_micro_key, get_handle_func);
  ret = ((OB_EAGAIN == ret) ? get_handle_func.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  ret = micro_map.operate(&micro_key, get_handle_func);
  ret = ((OB_EAGAIN == ret) ? get_handle_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, tmp_micro_handle.is_valid());
  ASSERT_EQ(ori_micro_ref_cnt + 3, tmp_micro_handle.get_ptr()->ref_cnt_);
  ASSERT_EQ(true, tmp_micro_handle.get_ptr()->is_valid());
  ASSERT_EQ(false, tmp_micro_handle.get_ptr()->is_persisted());
  ASSERT_EQ(true, mem_blk_handle.is_valid());
  ASSERT_EQ(ori_micro_ref_cnt + 2, mem_blk_handle.get_ptr()->ref_cnt_);
  ASSERT_EQ(false, phy_blk_handle.is_valid());
  tmp_micro_handle.reset();
  mem_blk_handle.reset();

  // invalidate this micro_block meta
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  micro_meta->mark_invalid();
  ASSERT_EQ(false, micro_meta->is_valid_field());
  ASSERT_EQ(0, micro_meta->first_val_);

  // check this invalid micro_block meta
  ret = micro_map.get(&micro_key, tmp_micro_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, tmp_micro_handle.is_valid());
  ASSERT_EQ(false, tmp_micro_handle()->is_valid_field());
  tmp_micro_handle.reset();

  // validate this micro_block meta
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  mem_blk_handle.set_ptr(&mem_blk);
  SSMicroMapUpdateMicroFunc update_func2(size, mem_blk_handle, crc);
  ret = micro_map.operate(&micro_key, update_func2);
  ret = ((OB_EAGAIN == ret) ? update_func2.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, micro_meta->is_valid());
  ASSERT_EQ(ObSSARCOpType::SS_ARC_HIT_T2, update_func.arc_op_type_);
  ASSERT_EQ((uint64_t)&mem_blk, micro_meta->data_dest_);
  ASSERT_EQ(micro_meta->reuse_version(), mem_blk.reuse_version_);
  ASSERT_EQ(false, micro_meta->is_in_l1_);
  ASSERT_EQ(false, micro_meta->is_in_ghost_);
  mem_blk_handle.reset();

  // mock this micro_block meta was persisted
  int64_t phy_blk_idx = 2;
  int64_t data_dest = phy_blk_idx * micro_meta_mgr.block_size_;
  ObSSPhysicalBlock * phy_blk = phy_blk_mgr.get_phy_block_by_idx_nolock(phy_blk_idx);
  ASSERT_NE(nullptr, phy_blk);
  int64_t phy_reuse_version = 7;
  phy_blk->reuse_version_ = phy_reuse_version;
  mem_blk_handle.set_ptr(&mem_blk);
  SSMicroMapUpdateMicroDestFunc update_dest_func(data_dest, phy_reuse_version, mem_blk_handle);
  micro_meta->is_persisted_ = true; // mock a situation: there exists dup micro, the first is persisted, the second no need to persist
  ret = micro_map.operate(&micro_key, update_dest_func);
  ret = ((OB_EAGAIN == ret) ? update_dest_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, update_dest_func.succ_update_);

  micro_meta->is_persisted_ = false;
  SSMicroMapUpdateMicroDestFunc update_dest_func1(data_dest, phy_reuse_version, mem_blk_handle);
  ret = micro_map.operate(&micro_key, update_dest_func1);
  ret = ((OB_EAGAIN == ret) ? update_dest_func1.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, update_dest_func1.succ_update_);
  ASSERT_EQ(true, micro_meta->is_persisted_);
  ASSERT_EQ(phy_reuse_version, micro_meta->reuse_version_);
  ASSERT_EQ(data_dest, micro_meta->data_dest_);
  mem_blk_handle.reset();

  // mark this micro_block meta is reorganizing
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  ASSERT_EQ(true, micro_meta->is_persisted_valid());
  SSMicroMapTryReorganizeMicroFunc reorgan_func(data_dest, tmp_micro_handle);
  ret = micro_map.operate(&micro_key, reorgan_func);
  ret = ((OB_EAGAIN == ret) ? reorgan_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, tmp_micro_handle.is_valid());
  ASSERT_EQ(true, tmp_micro_handle.get_ptr()->is_reorganizing_);
  ASSERT_EQ(false, tmp_micro_handle.get_ptr()->is_in_l1_);
  ASSERT_EQ(false, tmp_micro_handle.get_ptr()->is_in_ghost_);
  tmp_micro_handle.reset();
  // mark it again
  ret = micro_map.operate(&micro_key, reorgan_func);
  ret = ((OB_EAGAIN == ret) ? reorgan_func.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret); // cuz is_reorganizing=true

  // mark this micro_block finish reorganize, mark is_reorganizing=false(mock this micro from T2->T1 during this time)
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  micro_meta->is_in_l1_ = true;
  mem_blk_handle.set_ptr(&mem_blk);
  SSMicroMapCompleteReorganizingMicroFunc update_reorgan_func(mem_blk_handle);
  ret = micro_map.operate(&micro_key, update_reorgan_func);
  ret = ((OB_EAGAIN == ret) ? update_reorgan_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, micro_meta->is_reorganizing_);
  ASSERT_EQ(false, micro_meta->is_persisted_);
  ASSERT_EQ(true, micro_meta->is_in_l1_);
  mem_blk_handle.reset();
  // update it again
  mem_blk_handle.set_ptr(&mem_blk);
  SSMicroMapCompleteReorganizingMicroFunc update_reorgan_func2(mem_blk_handle);
  ret = micro_map.operate(&micro_key, update_reorgan_func2);
  ret = ((OB_EAGAIN == ret) ? update_reorgan_func2.ret_ : ret);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret); // cuz micro_meta not consistent
  mem_blk_handle.reset();

  // mock this micro_block meta was persisted again
  phy_blk_idx = 11;
  data_dest = phy_blk_idx * micro_meta_mgr.block_size_;
  phy_reuse_version = phy_blk_mgr.get_phy_block_by_idx_nolock(phy_blk_idx)->reuse_version_;
  mem_blk_handle.set_ptr(&mem_blk);
  SSMicroMapUpdateMicroDestFunc update_dest_func2(data_dest, phy_reuse_version, mem_blk_handle);
  ret = micro_map.operate(&micro_key, update_dest_func2);
  ret = ((OB_EAGAIN == ret) ? update_dest_func2.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, micro_meta->is_persisted_valid());
  ASSERT_EQ(true, micro_meta->is_in_l1_);
  ASSERT_EQ(false, micro_meta->is_in_ghost_);
  ASSERT_EQ(phy_reuse_version, micro_meta->reuse_version());
  mem_blk_handle.reset();

  // evict this micro_block meta
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  ObSSMicroBlockMetaHandle evict_micro_handle;
  evict_micro_handle.set_ptr(micro_meta);
  SSMicroMapEvictMicroFunc evict_func(evict_micro_handle);
  ret = micro_map.operate(&micro_key, evict_func);
  ret = ((OB_EAGAIN == ret) ? evict_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, micro_meta->is_valid());
  ASSERT_EQ(true, micro_meta->is_in_ghost_);
  evict_micro_handle.reset();

  // delete this micro_block meta
  ObSSMicroBlockMeta delete_micro_meta = *micro_meta;
  delete_micro_meta.ref_cnt_ = 10;
  ObSSMicroBlockMetaHandle delete_micro_handle;
  delete_micro_handle.set_ptr(&delete_micro_meta);
  SSMicroMapDeleteMicroFunc delete_func(delete_micro_handle);
  micro_handle.reset();
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta->ref_cnt_);
  ASSERT_EQ(OB_SUCCESS, micro_map.erase_if(&micro_key, delete_func));
  delete_micro_handle.reset();

  // get this micro_block meta, not exist now.
  ret = micro_map.get(&micro_key, tmp_micro_handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  tmp_micro_handle.reset();
}

TEST_F(TestSSMicroCacheBasicOp, macro_map_func2)
{
  ObArenaAllocator allocator;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroMetaManager::SSMicroMap &micro_map = micro_meta_mgr.micro_meta_map_;

  ObSSMemDataManager &mem_data_mgr = micro_cache->mem_data_mgr_;
  ObSSMemBlockPool &mem_blk_pool = mem_data_mgr.mem_block_pool_;
  micro_cache->task_runner_.is_inited_ = false;
  micro_cache->task_runner_.is_stopped_ = true;
  ob_usleep(1000 * 1000);

  MacroBlockId macro_id(0, 1001, 0);
  int64_t micro_size = 256;
  int64_t micro_cnt = 100;
  char *micro_data = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, micro_data);
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::gen_random_data(micro_data, micro_size));
  ObSSMemBlock *mem_blk_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_blk_ptr, true/*is_fg*/));
  ASSERT_NE(nullptr, mem_blk_ptr);
  ObSSMemBlock &mem_blk = *mem_blk_ptr;

  ObSSMemBlockHandle mem_blk_handle;
  mem_blk_handle.set_ptr(mem_blk_ptr);

  for (int64_t i = 1; i <= micro_cnt; ++i) {
    const int32_t offset = i * micro_size;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    uint32_t crc = 0;
    int32_t data_offset = -1;
    int32_t idx_offset = -1;
    ASSERT_EQ(OB_SUCCESS, mem_blk.calc_write_location(micro_key, micro_size, data_offset, idx_offset));
    ASSERT_EQ(OB_SUCCESS, mem_blk.write_micro_data(micro_key, micro_data, micro_size, data_offset, idx_offset, crc));

    bool succ_add_new = false;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, crc, mem_blk_handle, succ_add_new));
    ASSERT_EQ(i, micro_map.count());
    if (i <= micro_cnt / 2) {
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      micro_meta_handle.get_ptr()->is_persisted_ = true; // mock this micro_meta finish being persisted
      micro_meta_handle.get_ptr()->data_dest_ = micro_cache->phy_block_size_ * 3;
    }
  }

  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_delete_micro_block_meta(mem_blk_handle));
  ASSERT_EQ(micro_cnt / 2, micro_map.count());

  for (int64_t i = 1; i <= micro_cnt / 2; ++i) {
    const int32_t offset = i * micro_size;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
    ASSERT_EQ(true, micro_meta_handle.is_valid());
    ASSERT_EQ(micro_key, micro_meta_handle()->micro_key());
    ASSERT_EQ(true, micro_meta_handle()->is_persisted());

    micro_meta_handle.get_ptr()->is_persisted_ = false; // revise it again, ready to delete it.
    micro_meta_handle.get_ptr()->inner_set_mem_block(mem_blk_handle);
  }

  mem_blk_handle.reset();
  mem_blk_handle.set_ptr(mem_blk_ptr);
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_delete_micro_block_meta(mem_blk_handle));
  ASSERT_EQ(0, micro_map.count());

  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&(micro_meta_->micro_key()), tmp_micro_meta_handle));
  ASSERT_EQ(1, micro_map.count());

  allocator.clear();
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_basic_op.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_basic_op.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}