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
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_meta_basic_op.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroMetaBasicOp : public ::testing::Test
{
public:
  TestSSMicroMetaBasicOp() : mem_blk_(nullptr), micro_meta_(nullptr) {}
  virtual ~TestSSMicroMetaBasicOp(){};
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

void TestSSMicroMetaBasicOp::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroMetaBasicOp::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroMetaBasicOp::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 31)));
  micro_cache->start();
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSMicroCache *)->mem_blk_mgr_.do_alloc_mem_block(mem_blk_, true));
  ASSERT_EQ(true, mem_blk_->is_valid());
  ObSSMemBlockHandle mem_handle;
  mem_handle.set_ptr(mem_blk_);
  ASSERT_EQ(OB_SUCCESS, alloc_and_init_micro_meta(micro_meta_, mem_handle));
  ASSERT_EQ(true, micro_meta_->is_valid());
}

void TestSSMicroMetaBasicOp::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  mem_blk_->destroy();
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

int TestSSMicroMetaBasicOp::alloc_and_init_micro_meta(
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
  const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
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
  } else if (OB_FAIL(tmp_micro_meta->init(micro_key, mem_blk_handle, size, crc, effective_tablet_id))) {
    LOG_WARN("fail to init micro_meta", KR(ret), K(micro_key), KPC(mem_blk_handle.get_ptr()));
  } else {
    ObSSMemBlock::ObSSMemMicroInfo mem_micro_info(offset, size);
    if (OB_FAIL(mem_blk_handle()->micro_loc_map_.set_refactored(micro_key, mem_micro_info, 1/*overwrite*/))) {
      LOG_WARN("fail to set refactored", KR(ret), K(micro_key), K(mem_micro_info));
    } else {
      mem_blk_handle()->update_valid_micro_info(size);
      micro_meta = tmp_micro_meta;
    }
  }
  return ret;
}

int TestSSMicroMetaBasicOp::mock_micro_block_persisted(ObSSMicroBlockMeta *micro_meta)
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
    ObSSPhyBlockHandle phy_handle;
    if (OB_FAIL(SSPhyBlockMgr.alloc_block(block_idx, phy_handle, ObSSPhyBlockType::SS_MICRO_DATA_BLK))) {
      LOG_WARN("fail to alloc phy_block", KR(ret));
    } else if (OB_UNLIKELY(!phy_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_handle is invalid", KR(ret), K(block_idx), K(phy_handle));
    } else {
      micro_meta->is_data_persisted_ = true;
      micro_meta->reuse_version_ = phy_handle.get_ptr()->reuse_version_;
      micro_meta->data_loc_ = block_idx * SSPhyBlockMgr.get_block_size() + 100;
    }
  }
  return ret;
}

TEST_F(TestSSMicroMetaBasicOp, get_micro_handle_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->get_micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();
  int64_t reuse_version = micro_meta_->reuse_version_;
  int64_t data_loc = micro_meta_->data_loc_;

  // 1. micro_block is in mem_block, and is transferred to T2 after ‘get’
  ObSSMemBlockHandle mem_handle;
  ObSSPhyBlockHandle phy_handle;
  ObSSMicroBlockMetaHandle micro_handle;
  ASSERT_EQ(true, micro_meta_->is_in_l1_);
  micro_meta_->update_access_type(2); // MAJOR_COMPACTION_PREWARM_TYPE
  ObSSMicroCacheAccessInfo access_info;
  SSMicroMapGetMicroHandleFunc func1(micro_handle, mem_handle, phy_handle, true, access_info);
  ret = micro_map.operate(&micro_key, func1);
  ret = ((OB_EAGAIN == ret) ? func1.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(mem_handle.get_ptr(), mem_blk_);
  ASSERT_EQ(false, micro_meta_->is_in_l1_); // transfer to T2
  ASSERT_EQ(true, access_info.is_valid());
  ASSERT_EQ(ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE, access_info.access_type_);
  ASSERT_EQ(1, access_info.delta_cnt_);
  ASSERT_EQ(micro_meta_->size(), access_info.delta_size_);
  ASSERT_EQ(0, micro_meta_->access_type());

  // 2. micro_meta is marked deleted, return OB_ENTRY_NOT_EXIST
  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  micro_meta_->mark_deleted();
  access_info.reset();
  SSMicroMapGetMicroHandleFunc func2(micro_handle, mem_handle, phy_handle, true, access_info);
  ret = micro_map.operate(&micro_key, func2);
  ret = ((OB_EAGAIN == ret) ? func2.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  micro_meta_->is_meta_deleted_ = 0;

  // 3. micro_meta is marked invalid, return OB_ENTRY_NOT_EXIST
  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  micro_meta_->mark_invalid();
  access_info.reset();
  SSMicroMapGetMicroHandleFunc func3(micro_handle, mem_handle, phy_handle, true, access_info);
  ret = micro_map.operate(&micro_key, func3);
  ret = ((OB_EAGAIN == ret) ? func3.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // 4. micro_block(data) is persisted
  micro_meta_->reuse_version_ = reuse_version;
  micro_meta_->data_loc_ = data_loc;
  ASSERT_EQ(OB_SUCCESS, mock_micro_block_persisted(micro_meta_));
  ASSERT_EQ(true, micro_meta_->is_valid());
  reuse_version = micro_meta_->reuse_version_;
  data_loc = micro_meta_->data_loc_;

  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  access_info.reset();
  SSMicroMapGetMicroHandleFunc func4(micro_handle, mem_handle, phy_handle, true, access_info);
  ret = micro_map.operate(&micro_key, func4);
  ret = ((OB_EAGAIN == ret) ? func4.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. phy_block reuse_version dismatch, return OB_ENTRY_NOT_EXIST
  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  micro_meta_->reuse_version_ = 100;
  access_info.reset();
  SSMicroMapGetMicroHandleFunc func5(micro_handle, mem_handle, phy_handle, true, access_info);
  ret = micro_map.operate(&micro_key, func5);
  ret = ((OB_EAGAIN == ret) ? func5.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // 6.1 set update_arc=true, it will be transfered to T2
  bool update_arc = true;
  micro_meta_->reuse_version_ = reuse_version;
  micro_meta_->data_loc_ = data_loc;
  micro_meta_->is_in_l1_ = true;
  micro_meta_->is_in_ghost_ = false;
  ASSERT_EQ(true, micro_meta_->is_valid());
  ASSERT_EQ(true, micro_meta_->is_data_persisted_);
  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  access_info.reset();
  micro_meta_->update_access_type(1); // COMMON_IO_TYPE
  SSMicroMapGetMicroHandleFunc func6(micro_handle, mem_handle, phy_handle, update_arc, access_info);
  ret = micro_map.operate(&micro_key, func6);
  ret = ((OB_EAGAIN == ret) ? func6.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, micro_meta_->is_valid());
  ASSERT_EQ(false, micro_meta_->is_in_l1_);
  ASSERT_EQ(false, micro_meta_->is_in_ghost_);
  ASSERT_EQ(true, micro_meta_->is_data_persisted_);
  ASSERT_EQ(false, access_info.is_valid());
  ASSERT_EQ(1, micro_meta_->access_type());

  // 6.2 get micro_meta info
  ObSSMicroBlockMetaInfo micro_meta_info;
  SSMicroMapGetMicroInfoFunc get_info_func(micro_meta_info);
  ret = micro_map.operate(&micro_key, get_info_func);
  ret = ((OB_EAGAIN == ret) ? get_info_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObSSMicroBlockMetaInfo tmp_micro_meta_info;
  micro_meta_->get_micro_meta_info(tmp_micro_meta_info);
  ASSERT_EQ(true, micro_meta_info == tmp_micro_meta_info);
  ASSERT_EQ(false, micro_meta_->is_meta_serialized());

  // 7. set update_arc=false, it won't transfer seg.
  update_arc = false;
  micro_meta_->reuse_version_ = reuse_version;
  micro_meta_->data_loc_ = data_loc;
  micro_meta_->is_data_persisted_ = true;
  micro_meta_->is_in_l1_ = true;
  micro_meta_->is_in_ghost_ = false;
  ASSERT_EQ(true, micro_meta_->is_valid());
  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  access_info.reset();
  SSMicroMapGetMicroHandleFunc func7(micro_handle, mem_handle, phy_handle, update_arc, access_info);
  ret = micro_map.operate(&micro_key, func7);
  ret = ((OB_EAGAIN == ret) ? func7.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, micro_meta_->is_valid());
  ASSERT_EQ(true, micro_meta_->is_in_l1_);
  ASSERT_EQ(false, micro_meta_->is_in_ghost_);
  ASSERT_EQ(true, micro_meta_->is_data_persisted_);

  // 8. micro_block meta is persisted into disk
  ASSERT_EQ(false, micro_meta_->is_meta_serialized_);
  ASSERT_EQ(false, micro_meta_->is_meta_persisted_);
  ASSERT_EQ(false, micro_meta_->is_meta_dirty_);
  micro_meta_->set_meta_persisted(true);
  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  access_info.reset();
  SSMicroMapGetMicroHandleFunc func8(micro_handle, mem_handle, phy_handle, update_arc, access_info);
  ret = micro_map.operate(&micro_key, func8);
  ret = ((OB_EAGAIN == ret) ? func8.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, micro_meta_->is_in_l1_);
  ASSERT_EQ(false, micro_meta_->is_meta_dirty_); // cuz nothing changed

  mem_handle.reset();
  phy_handle.reset();
  micro_handle.reset();
  access_info.reset();
  update_arc = true;
  SSMicroMapGetMicroHandleFunc func9(micro_handle, mem_handle, phy_handle, update_arc, access_info);
  ret = micro_map.operate(&micro_key, func9);
  ret = ((OB_EAGAIN == ret) ? func9.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, micro_meta_->is_in_l1_);
  ASSERT_EQ(true, micro_meta_->is_meta_dirty_); // cuz is_in_l1 is changed
}

TEST_F(TestSSMicroMetaBasicOp, update_micro_heat_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->get_micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();
  uint64_t cur_access_time = micro_meta_->access_time();

  bool transfer_seg = true;
  const bool update_access_time = true;
  int64_t time_delta_s = 100;
  // 1. if hit T1, transfer seg and update heat
  SSMicroMapUpdateMicroHeatFunc func1(transfer_seg, update_access_time, time_delta_s);
  ret = micro_map.operate(&micro_key, func1);
  ret = ((OB_EAGAIN == ret) ? func1.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObSSMicroArcOpType::SS_ARC_HIT_T1, func1.arc_op_type_);
  ASSERT_EQ(false, micro_meta_->is_in_l1_);
  ASSERT_GT(micro_meta_->access_time(), cur_access_time);
  cur_access_time = micro_meta_->access_time();

  // 2. if hit ghost, don't transfer seg, just update heat
  micro_meta_->is_in_l1_ = true;
  micro_meta_->is_in_ghost_ = true;
  time_delta_s = 200;
  transfer_seg = false;
  SSMicroMapUpdateMicroHeatFunc func2(transfer_seg, update_access_time, time_delta_s);
  ret = micro_map.operate(&micro_key, func2);
  ret = ((OB_EAGAIN == ret) ? func2.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObSSMicroArcOpType::SS_INVALID_TYPE, func2.arc_op_type_);
  ASSERT_GT(micro_meta_->access_time(), cur_access_time);
  ASSERT_EQ(true, micro_meta_->is_in_l1_);

  // 3. persist micro_meta, and hit T1
  micro_meta_->is_in_l1_ = true;
  micro_meta_->is_in_ghost_ = false;
  micro_meta_->is_meta_persisted_ = true;
  ASSERT_EQ(false, micro_meta_->is_meta_dirty_);
  transfer_seg = true;
  SSMicroMapUpdateMicroHeatFunc func3(transfer_seg, update_access_time, time_delta_s);
  ret = micro_map.operate(&micro_key, func3);
  ret = ((OB_EAGAIN == ret) ? func3.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, micro_meta_->is_in_l1_);
  ASSERT_EQ(true, micro_meta_->is_meta_dirty_);
}

TEST_F(TestSSMicroMetaBasicOp, deleted_unpersisted_micro_func)
{
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = micro_meta_mgr.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->get_micro_key();
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
  micro_meta_->is_data_persisted_ = true;
  SSMicroMapDeleteUnpersistedMicroFunc func1(mem_handle);
  ASSERT_EQ(OB_SUCCESS, micro_map.operate(&micro_key, func1));
  ASSERT_EQ(false, func1.succ_delete_);
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta_->ref_cnt_);

  // 2. fail to delete micro_block which was already deleted
  micro_meta_->is_data_persisted_ = false;
  micro_meta_->is_meta_deleted_ = true;
  SSMicroMapDeleteUnpersistedMicroFunc func2(mem_handle);
  ASSERT_EQ(OB_EAGAIN, micro_map.operate(&micro_key, func2));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, func2.ret_);

  // 3. succeed to delete micro_block which is not persisted
  micro_meta_->is_meta_deleted_ = false;
  SSMicroMapDeleteUnpersistedMicroFunc func3(mem_handle);
  ASSERT_EQ(OB_SUCCESS, micro_map.operate(&micro_key, func3));
  ASSERT_EQ(true, func3.succ_delete_);
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta_->ref_cnt_);
  ASSERT_EQ(true, micro_meta_->is_meta_deleted_);
  ASSERT_EQ(micro_meta_->size_, func3.size_);
  ASSERT_EQ(micro_meta_->is_in_l1_, func3.is_in_l1_);
  ASSERT_EQ(micro_meta_->is_in_ghost_, func3.is_in_ghost_);

  // 4. clear micro_meta_map
  micro_handle.reset();
  ASSERT_EQ(1, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
  micro_meta_mgr.clear();
  ASSERT_EQ(0, SSMicroCacheStat.micro_stat().get_valid_micro_cnt());
}

TEST_F(TestSSMicroMetaBasicOp, force_delete_micro_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->get_micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();

  ASSERT_EQ(true, micro_meta_->is_valid_field());
  ASSERT_EQ(true, micro_meta_->is_in_l1_);
  ASSERT_EQ(false, micro_meta_->is_in_ghost_);

  micro_meta_->is_meta_deleted_ = true;
  ObSSMicroBlockMetaInfo micro_meta_info;
  SSMicroMapForceDeleteMicroFunc delete_func(micro_meta_info);
  ret = micro_map.operate(&micro_key, delete_func);
  ASSERT_EQ(OB_EAGAIN, ret);
  ret = ((OB_EAGAIN == ret) ? delete_func.ret_ : ret);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_EQ(false, delete_func.succ_delete_);

  micro_meta_->is_meta_deleted_ = false;
  ObSSMicroBlockMetaInfo micro_meta_info1;
  SSMicroMapForceDeleteMicroFunc delete_func1(micro_meta_info1);
  ret = micro_map.operate(&micro_key, delete_func1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, delete_func1.succ_delete_);
  ASSERT_EQ(true, micro_meta_->is_meta_deleted_);
}

TEST_F(TestSSMicroMetaBasicOp, force_erase_micro_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->get_micro_key();
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();

  ASSERT_EQ(true, micro_meta_->is_valid_field());
  ASSERT_EQ(true, micro_meta_->is_in_l1_);
  ASSERT_EQ(false, micro_meta_->is_in_ghost_);

  const int64_t expiration_time_s = 48 * 3600L;
  const uint64_t tablet_id = micro_meta_->effective_tablet_id();
  micro_meta_->is_meta_deleted_ = true;
  ObSSMicroBlockMetaInfo micro_meta_info;
  SSMicroMapForceEraseMicroFunc erase_func(expiration_time_s, tablet_id, micro_meta_info);
  ret = micro_map.operate(&micro_key, erase_func);
  ASSERT_EQ(OB_EAGAIN, ret);

  micro_meta_->is_meta_deleted_ = false;
  micro_meta_->access_time_s_ -= expiration_time_s;
  ASSERT_LT(0, micro_meta_->access_time_s_);
  ObSSMicroBlockMetaInfo micro_meta_info1;
  SSMicroMapForceEraseMicroFunc erase_func1(expiration_time_s, tablet_id, micro_meta_info1);
  ret = micro_map.operate(&micro_key, erase_func1);
  ASSERT_EQ(OB_EAGAIN, ret);

  micro_meta_->access_time_s_ += expiration_time_s;
  ObSSMicroBlockMetaInfo micro_meta_info2;
  SSMicroMapForceEraseMicroFunc erase_func2(expiration_time_s, tablet_id + 1, micro_meta_info2);
  ret = micro_map.operate(&micro_key, erase_func2);
  ASSERT_EQ(OB_EAGAIN, ret);
  ret = ((OB_EAGAIN == ret) ? erase_func2.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMicroBlockMetaInfo micro_meta_info3;
  SSMicroMapForceEraseMicroFunc erase_func3(expiration_time_s, tablet_id, micro_meta_info3);
  ret = micro_map.operate(&micro_key, erase_func3);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestSSMicroMetaBasicOp, erase_deleted_micro_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->get_micro_key();
  ObSSMicroBlockCacheKey cp_micro_key = micro_key;
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();

  ASSERT_EQ(true, micro_meta_->is_valid_field());
  ASSERT_EQ(true, micro_meta_->is_in_l1_);
  ASSERT_EQ(false, micro_meta_->is_in_ghost_);

  micro_meta_->is_meta_deleted_ = false;
  SSMicroMapEraseDeletedMicroFunc erase_func;
  ret = micro_map.erase_if(&micro_key, erase_func);
  ASSERT_EQ(OB_EAGAIN, ret);
  ret = ((OB_EAGAIN == ret) ? erase_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);

  micro_meta_->is_meta_deleted_ = true;
  SSMicroMapEraseDeletedMicroFunc erase_func1;
  ret = micro_map.erase_if(&micro_key, erase_func1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = micro_map.get(&cp_micro_key, tmp_micro_meta_handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestSSMicroMetaBasicOp, erase_expired_micro_func)
{
  int ret = OB_SUCCESS;
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->get_micro_key();
  ObSSMicroBlockCacheKey cp_micro_key = micro_key;
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();

  ASSERT_EQ(true, micro_meta_->is_valid_field());
  ASSERT_EQ(true, micro_meta_->is_in_l1_);
  ASSERT_EQ(false, micro_meta_->is_in_ghost_);

  ASSERT_EQ(false, micro_meta_->is_expired(SS_DEF_CACHE_EXPIRATION_TIME_S));
  ObSSMicroBlockMetaInfo micro_meta_info;
  SSMicroMapEraseExpiredMicroFunc erase_func(SS_DEF_CACHE_EXPIRATION_TIME_S, micro_meta_info);
  ret = micro_map.erase_if(&micro_key, erase_func);
  ASSERT_EQ(OB_EAGAIN, ret);
  ret = ((OB_EAGAIN == ret) ? erase_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);

  micro_meta_->is_meta_deleted_ = true;
  micro_meta_info.reset();
  SSMicroMapEraseExpiredMicroFunc erase_func1(SS_DEF_CACHE_EXPIRATION_TIME_S, micro_meta_info);
  ret = micro_map.erase_if(&micro_key, erase_func1);
  ASSERT_EQ(OB_EAGAIN, ret);
  ret = ((OB_EAGAIN == ret) ? erase_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);

  micro_meta_->is_meta_deleted_ = false;
  micro_meta_->access_time_s_ = 1;
  micro_meta_info.reset();
  ASSERT_EQ(true, micro_meta_->is_expired(SS_DEF_CACHE_EXPIRATION_TIME_S));
  SSMicroMapEraseExpiredMicroFunc erase_func2(SS_DEF_CACHE_EXPIRATION_TIME_S, micro_meta_info);
  ret = micro_map.erase_if(&micro_key, erase_func2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, micro_meta_info.is_valid_field());
  ret = micro_map.get(&cp_micro_key, tmp_micro_meta_handle);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestSSMicroMetaBasicOp, erase_abnormal_micro_func)
{
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = MTL(ObSSMicroCache *)->micro_meta_mgr_.micro_meta_map_;
  const ObSSMicroBlockCacheKey &micro_key = micro_meta_->get_micro_key();
  ObSSMicroBlockCacheKey cp_micro_key = micro_key;
  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&micro_key, tmp_micro_meta_handle));
  tmp_micro_meta_handle.reset();

  ObSSMicroBlockMetaHandle micro_handle;
  micro_handle.set_ptr(micro_meta_);
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta_->ref_cnt_);

  // 1. fail to erase valid micro_meta
  SSMicroMapEraseAbnormalMicroFunc func1;
  ASSERT_EQ(OB_EAGAIN, micro_map.erase_if(&micro_key, func1));

  // 2. fail to erase ghost micro_meta
  micro_meta_->is_in_ghost_ = true;
  SSMicroMapEraseAbnormalMicroFunc func2;
  ASSERT_EQ(OB_EAGAIN, micro_map.erase_if(&micro_key, func2));

  // 3. succeed to erase T1/T2 invalid micro_meta
  micro_meta_->is_in_ghost_ = false;
  micro_meta_->mark_invalid();
  SSMicroMapEraseAbnormalMicroFunc func3;
  ASSERT_EQ(OB_SUCCESS, micro_map.erase_if(&micro_key, func3));
  ASSERT_EQ(ori_micro_ref_cnt + 1, micro_meta_->ref_cnt_);
  ASSERT_EQ(micro_meta_->size_, func3.size_);
  ASSERT_EQ(micro_meta_->is_in_l1_, func3.is_in_l1_);
  ASSERT_EQ(micro_meta_->is_in_ghost_, func3.is_in_ghost_);

  ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_map.get(&cp_micro_key, tmp_micro_meta_handle));

  micro_handle.reset();
  ASSERT_EQ(0, SSMicroCacheStat.mem_stat().get_micro_alloc_cnt());
}

TEST_F(TestSSMicroMetaBasicOp, micro_map_func)
{
  int ret = OB_SUCCESS;
  const uint32_t ori_micro_ref_cnt = ORI_MICRO_REF_CNT;
  ObArenaAllocator allocator;
  ObSSMicroMetaManager &micro_meta_mgr = MTL(ObSSMicroCache *)->micro_meta_mgr_;
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = micro_meta_mgr.micro_meta_map_;
  ObSSMicroBlockMetaHandle ori_micro_meta_handle;
  ori_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&(micro_meta_->get_micro_key()), ori_micro_meta_handle));
  ori_micro_meta_handle.reset();

  MacroBlockId macro_id(0, 100, 0);
  int64_t offset = 1;
  int64_t size = 256;
  ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, size);
  const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
  char *micro_data = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, micro_data);
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::gen_random_data(micro_data, size));

  ObSSMemBlock *mem_blk_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSMicroCache *)->mem_blk_mgr_.do_alloc_mem_block(mem_blk_ptr, true/*is_fg*/));
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
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.add_or_update_micro_block_meta(micro_key, size, crc,
                                       effective_tablet_id, mem_blk_handle, succ_add_new));
  ASSERT_EQ(true, succ_add_new);

  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(micro_key, micro_handle, ObTabletID::INVALID_TABLET_ID, false));
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_handle.get_ptr()->ref_cnt_);
  ObSSMicroBlockMeta *micro_meta = micro_handle.get_ptr();
  ASSERT_EQ(size, mem_blk.valid_val_);
  ASSERT_EQ(true, mem_blk.can_flush());
  mem_blk_handle.reset();

  // update effective tablet_id
  const uint64_t cur_tablet_id = micro_key.get_macro_tablet_id().id();
  const uint64_t new_tablet_id = cur_tablet_id + 1;
  SSMicroMapUpdateEffectiveTabletID update_tablet_id_func(new_tablet_id);
  ret = micro_map.operate(&micro_key, update_tablet_id_func);
  ret = ((OB_EAGAIN == ret) ? update_tablet_id_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(new_tablet_id, micro_handle()->effective_tablet_id());

  // get micro_meta, already exist, and it will be in T2
  ObSSMicroBlockCacheKey tmp_micro_key = micro_key;
  ObSSMicroBlockMetaHandle tmp_micro_handle;
  ret = micro_map.get(&micro_key, tmp_micro_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, tmp_micro_handle.is_valid());
  tmp_micro_handle()->transfer_arc_seg(ObSSMicroArcOpType::SS_ARC_HIT_T1);
  ASSERT_EQ(ori_micro_ref_cnt + 3, tmp_micro_handle.get_ptr()->ref_cnt_);
  ASSERT_EQ(true, tmp_micro_handle.get_ptr()->is_valid());
  ASSERT_EQ(micro_meta, tmp_micro_handle.get_ptr());
  ASSERT_EQ(false, micro_meta->is_in_l1_);
  ASSERT_EQ(false, micro_meta->is_in_ghost_);
  ASSERT_EQ(false, micro_meta->is_data_persisted_);
  tmp_micro_handle.reset();
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_handle.get_ptr()->ref_cnt_);

  // write the same micro_block data into mem_block again
  data_offset = -1;
  idx_offset = -1;
  ASSERT_EQ(OB_SUCCESS, mem_blk.calc_write_location(micro_key, size, data_offset, idx_offset));
  ASSERT_EQ(OB_SUCCESS, mem_blk.write_micro_data(micro_key, micro_data, size, data_offset, idx_offset, crc));

  // this micro_block meta already exist, we try to update it, actually treat it as 'HIT_T2'
  mem_blk_handle.set_ptr(&mem_blk);
  uint64_t access_type = 1;
  SSMicroMapUpdateMicroFunc update_func(size, mem_blk_handle, crc, effective_tablet_id, access_type);
  ret = micro_map.operate(&micro_key, update_func);
  ret = ((OB_EAGAIN == ret) ? update_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObSSMicroArcOpType::SS_ARC_HIT_T2, update_func.arc_op_type_);
  ASSERT_EQ(false, micro_meta->is_in_l1_);
  ASSERT_EQ(false, micro_meta->is_in_ghost_);
  mem_blk_handle.reset();

  // get this micro_block meta location
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  ObSSPhyBlockHandle phy_blk_handle;
  ObSSMicroCacheAccessInfo access_info;
  SSMicroMapGetMicroHandleFunc get_handle_func(tmp_micro_handle, mem_blk_handle, phy_blk_handle, false, access_info);
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
  ASSERT_EQ(false, tmp_micro_handle.get_ptr()->is_data_persisted());
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
  access_type = 1;
  SSMicroMapUpdateMicroFunc update_func2(size, mem_blk_handle, crc, effective_tablet_id, access_type);
  ret = micro_map.operate(&micro_key, update_func2);
  ret = ((OB_EAGAIN == ret) ? update_func2.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, micro_meta->is_valid());
  ASSERT_EQ(ObSSMicroArcOpType::SS_ARC_HIT_T2, update_func.arc_op_type_);
  ASSERT_EQ((uint64_t)&mem_blk, micro_meta->data_loc_);
  ASSERT_EQ(false, micro_meta->is_in_l1_);
  ASSERT_EQ(false, micro_meta->is_in_ghost_);
  mem_blk_handle.reset();

  // mock this micro_block meta was persisted
  int64_t phy_blk_idx = 2;
  int64_t data_loc = phy_blk_idx * micro_meta_mgr.block_size_;
  ObSSPhysicalBlock * phy_blk = phy_blk_mgr.inner_get_phy_block_by_idx(phy_blk_idx);
  ASSERT_NE(nullptr, phy_blk);
  int64_t phy_reuse_version = 7;
  phy_blk->reuse_version_ = phy_reuse_version;
  mem_blk_handle.set_ptr(&mem_blk);
  SSMicroMapUpdateMicroDestFunc update_dest_func(data_loc, phy_reuse_version, mem_blk_handle);
  micro_meta->is_data_persisted_ = true; // mock a situation: there exists dup micro, the first is persisted, the second no need to persist
  ret = micro_map.operate(&micro_key, update_dest_func);
  ret = ((OB_EAGAIN == ret) ? update_dest_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, update_dest_func.succ_update_);

  micro_meta->is_data_persisted_ = false;
  SSMicroMapUpdateMicroDestFunc update_dest_func1(data_loc, phy_reuse_version, mem_blk_handle);
  ret = micro_map.operate(&micro_key, update_dest_func1);
  ret = ((OB_EAGAIN == ret) ? update_dest_func1.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, update_dest_func1.succ_update_);
  ASSERT_EQ(true, micro_meta->is_data_persisted_);
  ASSERT_EQ(phy_reuse_version, micro_meta->reuse_version_);
  ASSERT_EQ(data_loc, micro_meta->data_loc_);
  mem_blk_handle.reset();

  // mark this micro_block meta is reorganizing
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  ASSERT_EQ(true, (micro_meta->is_data_persisted_ && micro_meta->is_valid()));
  mem_blk_handle.set_ptr(&mem_blk);
  ASSERT_EQ(true, micro_meta->can_reorganize());
  int64_t exp_data_loc = data_loc + 1; // set a wrong data_loc
  uint32_t exp_crc = crc;
  uint64_t exp_reuse_version = phy_reuse_version;
  SSMicroMapUpdateReorganizingMicroFunc reorgan_func(exp_data_loc, exp_crc, exp_reuse_version, mem_blk_handle);
  ret = micro_map.operate(&micro_key, reorgan_func);
  ret = ((OB_EAGAIN == ret) ? reorgan_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, reorgan_func.succ_update_);

  exp_data_loc = data_loc;
  exp_crc = crc + 1; // set a wrong crc
  SSMicroMapUpdateReorganizingMicroFunc reorgan_func0(exp_data_loc, exp_crc, exp_reuse_version, mem_blk_handle);
  ret = micro_map.operate(&micro_key, reorgan_func0);
  ret = ((OB_EAGAIN == ret) ? reorgan_func0.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, reorgan_func0.succ_update_);

  exp_data_loc = data_loc;
  exp_crc = crc;
  SSMicroMapUpdateReorganizingMicroFunc reorgan_func1(exp_data_loc, exp_crc, exp_reuse_version, mem_blk_handle);
  ret = micro_map.operate(&micro_key, reorgan_func1);
  ret = ((OB_EAGAIN == ret) ? reorgan_func1.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, reorgan_func1.succ_update_);
  ASSERT_EQ(false, micro_meta->is_in_l1_);
  ASSERT_EQ(false, micro_meta->is_in_ghost_);
  ASSERT_EQ(false, micro_meta->is_data_persisted_);
  ASSERT_EQ(false, micro_meta->can_reorganize());
  // mark it again
  SSMicroMapUpdateReorganizingMicroFunc reorgan_func2(exp_data_loc, exp_crc, exp_reuse_version, mem_blk_handle);
  ret = micro_map.operate(&micro_key, reorgan_func2);
  ret = ((OB_EAGAIN == ret) ? reorgan_func2.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, reorgan_func2.succ_update_);

  // mock this micro_block meta was persisted again
  phy_blk_idx = 11;
  data_loc = phy_blk_idx * micro_meta_mgr.block_size_;
  phy_reuse_version = phy_blk_mgr.inner_get_phy_block_by_idx(phy_blk_idx)->reuse_version_;
  mem_blk_handle.set_ptr(&mem_blk);
  SSMicroMapUpdateMicroDestFunc update_dest_func2(data_loc, phy_reuse_version, mem_blk_handle);
  ret = micro_map.operate(&micro_key, update_dest_func2);
  ret = ((OB_EAGAIN == ret) ? update_dest_func2.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, (micro_meta->is_data_persisted_ && micro_meta->is_valid()));
  ASSERT_EQ(false, micro_meta->is_in_l1_);
  ASSERT_EQ(false, micro_meta->is_in_ghost_);
  ASSERT_EQ(phy_reuse_version, micro_meta->reuse_version());
  mem_blk_handle.reset();

  // evict this micro_block meta
  ASSERT_EQ(ori_micro_ref_cnt + 2, micro_meta->ref_cnt_);
  ObSSMicroBlockMetaInfo evict_micro_info;
  micro_meta->get_micro_meta_info(evict_micro_info);
  SSMicroMapEvictMicroFunc evict_func(evict_micro_info);
  ret = micro_map.operate(&micro_key, evict_func);
  ret = ((OB_EAGAIN == ret) ? evict_func.ret_ : ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, micro_meta->is_valid());
  ASSERT_EQ(true, micro_meta->is_in_ghost_);

  // delete this micro_block meta
  ObSSMicroBlockMetaInfo delete_micro_info;
  micro_meta->get_micro_meta_info(delete_micro_info);
  SSMicroMapDeleteMicroFunc delete_func(delete_micro_info);
  micro_handle.reset();
  ASSERT_EQ(OB_SUCCESS, micro_map.operate(&micro_key, delete_func));
  ASSERT_EQ(true, micro_meta->is_meta_deleted_);
  ret = micro_meta_mgr.get_micro_block_meta(micro_key, tmp_micro_handle, ObTabletID::INVALID_TABLET_ID, false);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  tmp_micro_handle.reset();
}

TEST_F(TestSSMicroMetaBasicOp, macro_map_func2)
{
  ObArenaAllocator allocator;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = micro_meta_mgr.micro_meta_map_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;

  ObSSMemBlockManager &mem_blk_mgr = micro_cache->mem_blk_mgr_;
  ObSSMemBlockPool &mem_blk_pool = mem_blk_mgr.mem_blk_pool_;
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
    const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, crc,
                                         effective_tablet_id, mem_blk_handle, succ_add_new));
    ASSERT_EQ(i, micro_map.count());
    if (i <= micro_cnt / 2) {
      ObSSMicroBlockMetaHandle micro_meta_handle;
      ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(micro_key, micro_meta_handle, ObTabletID::INVALID_TABLET_ID, false));
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      micro_meta_handle.get_ptr()->reuse_version_ = 1;
      micro_meta_handle.get_ptr()->is_data_persisted_ = true; // mock this micro_meta finish being persisted
      micro_meta_handle.get_ptr()->data_loc_ = micro_cache->phy_blk_size_ * 3;
    }
  }

  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_delete_micro_block_meta(mem_blk_handle));
  ASSERT_EQ(micro_cnt, micro_map.count());
  ASSERT_EQ(micro_cnt / 2, cache_stat.micro_stat().mark_del_micro_cnt_);

  for (int64_t i = 1; i <= micro_cnt / 2; ++i) {
    const int32_t offset = i * micro_size;
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(micro_key, micro_meta_handle, ObTabletID::INVALID_TABLET_ID, false));
    ASSERT_EQ(true, micro_meta_handle.is_valid());
    ASSERT_EQ(micro_key, micro_meta_handle()->get_micro_key());
    ASSERT_EQ(true, micro_meta_handle()->is_data_persisted());

    micro_meta_handle.get_ptr()->is_data_persisted_ = false; // revise it again, ready to delete it.
    micro_meta_handle.get_ptr()->inner_set_mem_block(mem_blk_handle);
  }

  mem_blk_handle.reset();
  mem_blk_handle.set_ptr(mem_blk_ptr);
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.try_delete_micro_block_meta(mem_blk_handle));
  ASSERT_EQ(micro_cnt, cache_stat.micro_stat().mark_del_micro_cnt_);

  ObSSMicroBlockMetaHandle tmp_micro_meta_handle;
  tmp_micro_meta_handle.set_ptr(micro_meta_);
  ASSERT_EQ(OB_SUCCESS, micro_map.insert(&(micro_meta_->get_micro_key()), tmp_micro_meta_handle));
  ASSERT_EQ(micro_cnt + 1, micro_map.count());

  micro_meta_mgr.clear();
  allocator.clear();
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_meta_basic_op.log*");
  OB_LOGGER.set_file_name("test_ss_micro_meta_basic_op.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}