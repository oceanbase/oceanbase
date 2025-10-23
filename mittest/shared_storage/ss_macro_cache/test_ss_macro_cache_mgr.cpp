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

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

class TestSSMacroCacheMgr : public ::testing::Test
{
public:
  TestSSMacroCacheMgr() {}
  virtual ~TestSSMacroCacheMgr() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void check_macro_cache_meta(const MacroBlockId &macro_id,
                              const ObSSMacroCacheInfo &cache_info);
public:
  static const int64_t WRITE_IO_SIZE = 2 * 1024L * 1024L; // 2MB
  ObStorageObjectWriteInfo write_info_;
  char write_buf_[WRITE_IO_SIZE];
};

void TestSSMacroCacheMgr::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestSSMacroCacheMgr::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMacroCacheMgr::SetUp()
{
  // construct write info
  write_buf_[0] = '\0';
  const int64_t mid_offset = WRITE_IO_SIZE / 2;
  memset(write_buf_, 'a', mid_offset);
  memset(write_buf_ + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
  write_info_.io_desc_.set_wait_event(1);
  write_info_.buffer_ = write_buf_;
  write_info_.offset_ = 0;
  write_info_.size_ = WRITE_IO_SIZE;
  write_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info_.mtl_tenant_id_ = MTL_ID();
}

void TestSSMacroCacheMgr::TearDown()
{
  write_buf_[0] = '\0';
}

void TestSSMacroCacheMgr::check_macro_cache_meta(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheInfo &cache_info)
{
  static int64_t call_times = 0;
  call_times++;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr) << "call_times: " << call_times;
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle)) << "call_times: " << call_times;
  ASSERT_TRUE(meta_handle.is_valid()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.effective_tablet_id_, meta_handle()->get_effective_tablet_id()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.size_, meta_handle()->get_size()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.cache_type_, meta_handle()->get_cache_type()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.is_write_cache_, meta_handle()->get_is_write_cache()) << "call_times: " << call_times;
}
TEST_F(TestSSMacroCacheMgr, put_update_erase)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200001;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(server_id);
  macro_id.set_macro_transfer_epoch(0); // transfer_seq
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // ObSSMacroCacheInfo
  uint64_t effective_tablet_id = 200001;
  uint32_t size = 1 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = true;
  ObSSMacroCacheInfo cache_info(effective_tablet_id, size, cache_type, is_write_cache);

  // 1. put
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put(macro_id, cache_info));
  check_macro_cache_meta(macro_id, cache_info);

  // 2. update
  ObSSMacroCacheUpdateParam update_param(true/*is_update_lru*/,
                                         true/*is_update_last_access_time*/,
                                         true/*is_update_effective_tablet_id*/,
                                         true/*is_update_size*/,
                                         false/*is_update_marked*/,
                                         false/*is_update_write_cache*/);
  cache_info.effective_tablet_id_ = 200002;
  cache_info.size_ = 2 * 1024 * 1024;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->update(macro_id, cache_info, update_param));
  check_macro_cache_meta(macro_id, cache_info);

  // 3. erase
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id));
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_HASH_NOT_EXIST, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
}

TEST_F(TestSSMacroCacheMgr, put_or_update_and_batch_update)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200001;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(server_id);
  macro_id.set_macro_transfer_epoch(0); // transfer_seq
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // ObSSMacroCacheInfo
  uint64_t effective_tablet_id = 200001;
  uint32_t size = 1 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = true;
  ObSSMacroCacheInfo cache_info(effective_tablet_id, size, cache_type, is_write_cache);

  // 1. put_or_update trigger put
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put_or_update(macro_id, cache_info));
  check_macro_cache_meta(macro_id, cache_info);

  // 2. put_or_update trigger update
  cache_info.effective_tablet_id_ = 200002;
  cache_info.size_ = 2 * 1024 * 1024;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put_or_update(macro_id, cache_info));
  check_macro_cache_meta(macro_id, cache_info);

  // 3. put_or_update another macro_id
  MacroBlockId macro_id_2;
  macro_id_2.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id_2.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id_2.set_second_id(tablet_id);
  macro_id_2.set_third_id(server_id);
  macro_id_2.set_macro_transfer_epoch(0); // transfer_seq
  macro_id_2.set_tenant_seq(101); // tenant_seq
  ASSERT_TRUE(macro_id_2.is_valid());

  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put_or_update(macro_id_2, cache_info));
  check_macro_cache_meta(macro_id_2, cache_info);

  // 4. batch update
  cache_info.effective_tablet_id_ = 200003;

  ObArray<MacroBlockId> macro_id_arr;
  ASSERT_EQ(OB_SUCCESS, macro_id_arr.push_back(macro_id));
  ASSERT_EQ(OB_SUCCESS, macro_id_arr.push_back(macro_id_2));
  ObSSMacroCacheUpdateParam update_param(true/*is_update_lru*/,
                                         true/*is_update_last_access_time*/,
                                         true/*is_update_effective_tablet_id*/,
                                         true/*is_update_size*/,
                                         false/*is_update_marked*/,
                                         false/*is_update_write_cache*/);
  for (int64_t i = 0; i < macro_id_arr.count(); ++i) {
    const MacroBlockId &cur_macro_id = macro_id_arr.at(i);
    ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->update(cur_macro_id, cache_info, update_param));
  }
  check_macro_cache_meta(macro_id, cache_info);
  check_macro_cache_meta(macro_id_2, cache_info);

  // 5. update_effective_tablet_id
  cache_info.effective_tablet_id_ = 200004;
  ObTabletID cur_tablet_id(cache_info.effective_tablet_id_);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->update_effective_tablet_id(macro_id_arr, cur_tablet_id));
  check_macro_cache_meta(macro_id, cache_info);
  check_macro_cache_meta(macro_id_2, cache_info);

  // 6. erase
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id));
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id_2));
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_HASH_NOT_EXIST, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
  ASSERT_EQ(OB_HASH_NOT_EXIST, macro_cache_mgr->meta_map_.get_refactored(macro_id_2, meta_handle));
}

TEST_F(TestSSMacroCacheMgr, update_effective_tablet_id_by_get)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200004;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(server_id);
  macro_id.set_macro_transfer_epoch(0); // transfer_seq
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // 1. write to macro cache
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // check macro cache meta
  uint64_t effective_tablet_id = tablet_id;
  uint32_t size = 2 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = true;
  ObSSMacroCacheInfo cache_info(effective_tablet_id, size, cache_type, is_write_cache);
  check_macro_cache_meta(macro_id, cache_info);

  // 2. get with new valid effective_tablet_id,
  //    expect macro_cache_meta's effective_tablet_id to be updated to new_effective_tablet_id
  bool is_hit_cache = false;
  ObSSFdCacheHandle fd_handle;
  ObTabletID new_effective_tablet_id(effective_tablet_id + 1);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, new_effective_tablet_id, size, is_hit_cache, fd_handle));
  ASSERT_EQ(TRUE, is_hit_cache);
  ObSSMacroCacheInfo new_cache_info(new_effective_tablet_id.id(), size, cache_type, is_write_cache);
  check_macro_cache_meta(macro_id, new_cache_info);
  fd_handle.reset();

  // 3. get with invalid effective_tablet_id,
  //    expect macro_cache_meta's effective_tablet_id to remain unchanged
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID), size, is_hit_cache, fd_handle));
  ASSERT_EQ(TRUE, is_hit_cache);
  check_macro_cache_meta(macro_id, new_cache_info); // new_cache_info here is the same as above
  fd_handle.reset();

  // 4. erase
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id));
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_HASH_NOT_EXIST, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
}

TEST_F(TestSSMacroCacheMgr, shift_lru_list)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tnt_disk_space_mgr);

  const uint64_t tablet_id = 200005;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(server_id);
  macro_id.set_macro_transfer_epoch(0); // transfer_seq
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // 1. write to macro cache
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // check macro cache meta
  uint32_t size = 2 * 1024 * 1024;
  ObSSMacroCacheInfo cache_info(tablet_id, size, ObSSMacroCacheType::MACRO_BLOCK, true/*is_write_cache*/);
  check_macro_cache_meta(macro_id, cache_info);

  // check disk space
  ObSSMacroCacheStat macro_block_stat;
  ObSSMacroCacheStat hot_tablet_macro_block_stat;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK,
                                                                 macro_block_stat));
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                                                                 hot_tablet_macro_block_stat));
  const int64_t ori_macro_block_used = macro_block_stat.used_;
  const int64_t ori_hot_tablet_macro_block_used = hot_tablet_macro_block_stat.used_;

  // 2. modify macro cache type from MACRO_BLOCK to HOT_TABLET_MACRO_BLOCK
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->modify_macro_cache_type(
                                         macro_id,
                                         ObSSMacroCacheType::MACRO_BLOCK,
                                         ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));
  // check macro cache meta
  ObSSMacroCacheInfo new_cache_info(tablet_id, size, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, true/*is_write_cache*/);
  check_macro_cache_meta(macro_id, new_cache_info);

  // check disk space
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK,
                                                                 macro_block_stat));
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                                                                 hot_tablet_macro_block_stat));
  ASSERT_EQ(macro_block_stat.used_, ori_macro_block_used - size);
  ASSERT_EQ(hot_tablet_macro_block_stat.used_, ori_hot_tablet_macro_block_used + size);

  // 3. modify macro cache type from HOT_TABLET_MACRO_BLOCK to MACRO_BLOCK
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->modify_macro_cache_type(
                                         macro_id,
                                         ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                                         ObSSMacroCacheType::MACRO_BLOCK));
  // check macro cache meta
  check_macro_cache_meta(macro_id, cache_info);

  // check disk space
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK,
                                                                 macro_block_stat));
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                                                                 hot_tablet_macro_block_stat));
  ASSERT_EQ(macro_block_stat.used_, ori_macro_block_used);
  ASSERT_EQ(hot_tablet_macro_block_stat.used_, ori_hot_tablet_macro_block_used);

  // 4. erase
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id));
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_HASH_NOT_EXIST, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
}

TEST_F(TestSSMacroCacheMgr, update_lru_list)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tnt_disk_space_mgr);

  const uint64_t tablet_id = 200006;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(server_id);
  macro_id.set_macro_transfer_epoch(0); // transfer_seq
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  MacroBlockId macro_id_2;
  macro_id_2.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id_2.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id_2.set_second_id(tablet_id);
  macro_id_2.set_third_id(server_id);
  macro_id_2.set_macro_transfer_epoch(0); // transfer_seq
  macro_id_2.set_tenant_seq(101); // tenant_seq
  ASSERT_TRUE(macro_id_2.is_valid());

  // 1. write to macro cache
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id_2));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // check macro cache meta
  uint32_t size = 2 * 1024 * 1024;
  ObSSMacroCacheInfo cache_info(tablet_id, size, ObSSMacroCacheType::MACRO_BLOCK, true/*is_write_cache*/);
  check_macro_cache_meta(macro_id, cache_info);
  check_macro_cache_meta(macro_id_2, cache_info);
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_TRUE(meta_handle()->get_is_in_fifo_list());
  const int64_t last_access_time = meta_handle()->get_last_access_time_us(); // record last_access_time for compare below
  meta_handle.reset();
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id_2, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_TRUE(meta_handle()->get_is_in_fifo_list());
  meta_handle.reset();

  // 2. get
  for (int64_t i = 1; i <= ObSSMacroCacheMeta::UPDATE_LRU_LIST_THRESHOLD; ++i) {
    bool is_hit_cache = false;
    ObSSFdCacheHandle fd_handle;
    ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id), size, is_hit_cache, fd_handle));
    ASSERT_TRUE(is_hit_cache);
    ObSSMacroCacheMetaHandle meta_handle;
    ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
    ASSERT_TRUE(meta_handle.is_valid());
    if (i == ObSSMacroCacheMeta::UPDATE_LRU_LIST_THRESHOLD) {
      ASSERT_EQ(0, meta_handle()->access_cnt_);
    } else {
      ASSERT_EQ(i, meta_handle()->access_cnt_);
    }
  }
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_FALSE(meta_handle()->get_is_in_fifo_list());
  ASSERT_GT(meta_handle()->get_last_access_time_us(), last_access_time);
  meta_handle.reset();
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id_2, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_TRUE(meta_handle()->get_is_in_fifo_list());
  meta_handle.reset();
}

TEST_F(TestSSMacroCacheMgr, force_evict_by_macro_id_write_cache)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200007;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(server_id);
  macro_id.set_macro_transfer_epoch(0); // transfer_seq
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // 1. write to macro cache
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // check macro cache meta
  uint64_t effective_tablet_id = tablet_id;
  uint32_t size = 2 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = true;
  ObSSMacroCacheInfo cache_info(effective_tablet_id, size, cache_type, is_write_cache);
  check_macro_cache_meta(macro_id, cache_info);

  // 2. force evict the macro
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->force_evict_by_macro_id(macro_id));
  ASSERT_EQ(nullptr, macro_cache_mgr->meta_map_.get(macro_id));
}

TEST_F(TestSSMacroCacheMgr, clear_macro_cache)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200008;
  const uint64_t hot_tablet_id = 200009;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(server_id);
  macro_id.set_macro_transfer_epoch(0); // transfer_seq
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // 1.1 write non-HOT_TABLET_MACRO_BLOCK to macro cache
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  write_info_.set_is_write_cache(false); // simulate read_cache
  write_info_.set_effective_tablet_id(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 1.2 check macro cache meta
  uint32_t size = 2 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = false;
  ObSSMacroCacheInfo cache_info(tablet_id, size, cache_type, is_write_cache);
  check_macro_cache_meta(macro_id, cache_info);


  // 2.1 write HOT_TABLET_MACRO_BLOCK to macro cache
  MacroBlockId hot_macro_id(macro_id);
  hot_macro_id.set_second_id(hot_tablet_id);
  ASSERT_TRUE(hot_macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(hot_macro_id));
  write_info_.set_is_write_cache(false); // simulate read_cache
  write_info_.set_effective_tablet_id(hot_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 2.2 check macro cache meta
  ObSSMacroCacheInfo hot_cache_info(hot_tablet_id, size, ObSSMacroCacheType::MACRO_BLOCK, is_write_cache);
  check_macro_cache_meta(hot_macro_id, hot_cache_info);

  // 2.3 modify macro cache type from MACRO_BLOCK to HOT_TABLET_MACRO_BLOCK
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->modify_macro_cache_type(
                                         hot_macro_id,
                                         ObSSMacroCacheType::MACRO_BLOCK,
                                         ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));
  hot_cache_info.cache_type_ = ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK;
  check_macro_cache_meta(hot_macro_id, hot_cache_info);

  // 3. clear macro cache
  const int64_t abs_timeout_us = ObTimeUtility::current_time() + 10000000L; // 10s
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->clear_macro_cache(abs_timeout_us, false/*is_hot_tablet*/));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(hot_macro_id, is_exist));
  ASSERT_TRUE(is_exist);

  // 4. clear hot tablet macro cache
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->clear_macro_cache(abs_timeout_us, true/*is_hot_tablet*/));
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(hot_macro_id, is_exist));
  ASSERT_FALSE(is_exist);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_mgr.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
