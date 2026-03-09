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
#define protected public
#define private public
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "lib/future/ob_future.h"
#include "observer/ob_server.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::storage;
using namespace oceanbase::observer;

class TestObSSFlushWriteCache : public ::testing::Test
{
public:
  TestObSSFlushWriteCache() = default;
  virtual ~TestObSSFlushWriteCache() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  MacroBlockId gen_shared_mini_macro_id(const int64_t third_id);

public:
  static const uint64_t TABLET_ID = 200001;
  static const int64_t REORGANIZATION_SCN = 0;
  static const int64_t WRITE_IO_SIZE = 2 * 1024L * 1024L; // 2MB
  ObStorageObjectWriteInfo write_info_;
  ObStorageObjectReadInfo read_info_;
  char write_buf_[WRITE_IO_SIZE];
  char read_buf_[WRITE_IO_SIZE];
};

void TestObSSFlushWriteCache::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestObSSFlushWriteCache::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestObSSFlushWriteCache::SetUp()
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

  // construct read info
  read_buf_[0] = '\0';
  read_info_.io_desc_.set_wait_event(1);
  read_info_.buf_ = read_buf_;
  read_info_.offset_ = 0;
  read_info_.size_ = WRITE_IO_SIZE;
  read_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info_.mtl_tenant_id_ = MTL_ID();
}

void TestObSSFlushWriteCache::TearDown()
{
}

// Helper function to create a valid MacroBlockId
MacroBlockId TestObSSFlushWriteCache::gen_shared_mini_macro_id(const int64_t third_id)
{
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO);
  macro_id.set_second_id(TestObSSFlushWriteCache::TABLET_ID);
  macro_id.set_third_id(third_id);
  macro_id.set_fourth_id(TestObSSFlushWriteCache::REORGANIZATION_SCN);
  return macro_id;
}

TEST_F(TestObSSFlushWriteCache, without_meta_and_without_file)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  uint64_t third_id = 0;

  // Create a macro_id that doesn't exist in meta_map
  ASSERT_EQ(OB_SUCCESS, ObIncSSMacroSeqHelper::build_shared_mini_data_seq(
                        0/*source_type*/, 1/*op_id*/, 1/*macro_seq_id*/, third_id));
  MacroBlockId macro_id = gen_shared_mini_macro_id(third_id);
  ASSERT_TRUE(macro_id.is_valid());

  // Verify meta doesn't exist
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);

  // Create and init promise
  ObPromise<int> promise;
  ASSERT_EQ(OB_SUCCESS, promise.init());
  ASSERT_TRUE(promise.is_valid());

  // Call flush_write_cache
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->flush_write_cache(macro_id, promise));

  // Verify future will be set to OB_SUCCESS
  ObFuture<int> future = promise.get_future();
  ASSERT_EQ(OB_SUCCESS, future.wait_for(30000/*30s*/));
  int *res = nullptr;
  ASSERT_EQ(OB_SUCCESS, future.get(res));
  ASSERT_NE(nullptr, res);
  ASSERT_EQ(OB_SUCCESS, *res);
}

TEST_F(TestObSSFlushWriteCache, without_meta_but_with_file)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  uint64_t third_id = 0;

  ASSERT_EQ(OB_SUCCESS, ObIncSSMacroSeqHelper::build_shared_mini_data_seq(
                        0/*source_type*/, 1/*op_id*/, 2/*macro_seq_id*/, third_id));
  MacroBlockId macro_id = gen_shared_mini_macro_id(third_id);
  ASSERT_TRUE(macro_id.is_valid());

  // Write macro to local cache
  write_info_.set_effective_tablet_id(TABLET_ID);
  write_info_.set_is_write_cache(true);
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // Verify meta exists and is write cache
  ObSSMacroCacheMetaHandle meta_handle;
  bool meta_exist = false;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get_macro_cache_meta(macro_id, meta_exist, meta_handle));
  ASSERT_TRUE(meta_exist);
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_TRUE(meta_handle()->get_is_write_cache());
  meta_handle.reset();

  // Delete meta to simulate restart observer
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id));
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get_macro_cache_meta(macro_id, meta_exist, meta_handle));
  ASSERT_FALSE(meta_exist);

  // Create and init promise
  ObPromise<int> promise;
  ASSERT_EQ(OB_SUCCESS, promise.init());
  ASSERT_TRUE(promise.is_valid());

  // Call flush_write_cache
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->flush_write_cache(macro_id, promise));

  // Verify future will be set to OB_SUCCESS
  ObFuture<int> future = promise.get_future();
  ASSERT_EQ(OB_SUCCESS, future.wait_for(30000/*30s*/));
  int *res = nullptr;
  ASSERT_EQ(OB_SUCCESS, future.get(res));
  ASSERT_NE(nullptr, res);
  ASSERT_EQ(OB_SUCCESS, *res);

  // Verify macro is flushed to object storage
  ObTenantFileManager *tenant_file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tenant_file_manager);
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
}

TEST_F(TestObSSFlushWriteCache, with_write_cache_meta)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  uint64_t third_id = 0;

  // Create a macro_id
  ASSERT_EQ(OB_SUCCESS, ObIncSSMacroSeqHelper::build_shared_mini_data_seq(
                        0/*source_type*/, 1/*op_id*/, 3/*macro_seq_id*/, third_id));
  MacroBlockId macro_id = gen_shared_mini_macro_id(third_id);
  ASSERT_TRUE(macro_id.is_valid());

  // Write macro to local cache
  write_info_.set_effective_tablet_id(TABLET_ID);
  write_info_.set_is_write_cache(true);
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // Verify meta exists and is write cache
  ObSSMacroCacheMetaHandle meta_handle;
  bool meta_exist = false;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get_macro_cache_meta(macro_id, meta_exist, meta_handle));
  ASSERT_TRUE(meta_exist);
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_TRUE(meta_handle()->get_is_write_cache());
  meta_handle.reset();

  // Create and init promise
  ObPromise<int> promise;
  ASSERT_EQ(OB_SUCCESS, promise.init());
  ASSERT_TRUE(promise.is_valid());

  // Call flush_write_cache
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->flush_write_cache(macro_id, promise));

  // Verify future will be set to OB_SUCCESS
  ObFuture<int> future = promise.get_future();
  ASSERT_EQ(OB_SUCCESS, future.wait_for(30000/*30s*/));
  int *res = nullptr;
  ASSERT_EQ(OB_SUCCESS, future.get(res));
  ASSERT_NE(nullptr, res);
  ASSERT_EQ(OB_SUCCESS, *res);

  // Verify macro is flushed to object storage
  ObTenantFileManager *tenant_file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tenant_file_manager);
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // Verify meta is changed to read cache
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get_macro_cache_meta(macro_id, meta_exist, meta_handle));
  ASSERT_TRUE(meta_exist);
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_FALSE(meta_handle()->get_is_write_cache());
  meta_handle.reset();
}

TEST_F(TestObSSFlushWriteCache, with_read_cache_meta)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  uint64_t third_id = 0;

  // Create a macro_id
  ASSERT_EQ(OB_SUCCESS, ObIncSSMacroSeqHelper::build_shared_mini_data_seq(
                        0/*source_type*/,
                        1/*op_id*/,
                        4/*macro_seq_id*/,
                        third_id));
  MacroBlockId macro_id = gen_shared_mini_macro_id(third_id);
  ASSERT_TRUE(macro_id.is_valid());

  // Write macro to local cache
  write_info_.set_effective_tablet_id(TABLET_ID);
  write_info_.set_is_write_cache(false);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ObSSLocalCacheWriter local_cache_writer;
  ASSERT_EQ(OB_SUCCESS, local_cache_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // Verify meta exists and is read cache
  ObSSMacroCacheMetaHandle meta_handle;
  bool meta_exist = false;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get_macro_cache_meta(macro_id, meta_exist, meta_handle));
  ASSERT_TRUE(meta_exist);
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_FALSE(meta_handle()->get_is_write_cache());
  meta_handle.reset();

  // Create and init promise
  ObPromise<int> promise;
  ASSERT_EQ(OB_SUCCESS, promise.init());
  ASSERT_TRUE(promise.is_valid());

  // Call flush_write_cache
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->flush_write_cache(macro_id, promise));

  // Verify future will be set to OB_SUCCESS
  ObFuture<int> future = promise.get_future();
  ASSERT_EQ(OB_SUCCESS, future.wait_for(30000/*30s*/));
  int *res = nullptr;
  ASSERT_EQ(OB_SUCCESS, future.get(res));
  ASSERT_NE(nullptr, res);
  ASSERT_EQ(OB_SUCCESS, *res);

  // Verify macro is not flushed to object storage
  ObTenantFileManager *tenant_file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tenant_file_manager);
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_flush_write_cache.log*");
  OB_LOGGER.set_file_name("test_ss_flush_write_cache.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
