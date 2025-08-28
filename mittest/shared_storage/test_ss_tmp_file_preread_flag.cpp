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
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache.h"
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

class TestSSTmpFilePrereadFlag : public ::testing::Test
{
public:
  TestSSTmpFilePrereadFlag() {}
  virtual ~TestSSTmpFilePrereadFlag() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

public:
  static const int64_t WRITE_IO_SIZE = 2 * 1024 * 1024; // 2MB
  ObStorageObjectWriteInfo write_info_;
  ObStorageObjectReadInfo read_info_;
  char write_buf_[WRITE_IO_SIZE];
  char read_buf_[WRITE_IO_SIZE];
};

void TestSSTmpFilePrereadFlag::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestSSTmpFilePrereadFlag::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSTmpFilePrereadFlag::SetUp()
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
  write_info_.set_tmp_file_valid_length(WRITE_IO_SIZE);
  write_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info_.mtl_tenant_id_ = MTL_ID();

  // construct read info
  read_buf_[0] = '\0';
  read_info_.io_desc_.set_wait_event(1);
  read_info_.buf_ = read_buf_;
  read_info_.offset_ = 0;
  read_info_.size_ = 8192; // 8KB
  read_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info_.mtl_tenant_id_ = MTL_ID();
}

void TestSSTmpFilePrereadFlag::TearDown()
{
  write_buf_[0] = '\0';
  read_buf_[0] = '\0';
}

TEST_F(TestSSTmpFilePrereadFlag, preread)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);

  // 1. write
  const int64_t tmp_file_id = 100;
  const int64_t segment_id = 0;
  MacroBlockId macro_id;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id);
  macro_id.set_third_id(segment_id);
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.io_desc_.set_sealed();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_append_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  // check if write through to object storage, and do not write local cache
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);

  // 2. read with is_preread = true
  read_info_.macro_block_id_ = macro_id;
  read_info_.io_desc_.set_preread();
  ObStorageObjectHandle read_object_handle;
  ObSSTmpFileReader tmp_file_reader;
  ASSERT_EQ(OB_SUCCESS, tmp_file_reader.aio_read(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_, read_object_handle.get_buffer(), read_info_.size_));

  // 3. wait preread
  usleep(5 * 1000L * 1000L); // sleep 5s

  // 4. check if segment has been preread to local mem_macro_cache
  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, mem_macro_cache);
  ASSERT_EQ(OB_SUCCESS, mem_macro_cache->check_exist(macro_id, is_exist));
  ASSERT_TRUE(is_exist);
}

TEST_F(TestSSTmpFilePrereadFlag, no_preread)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);

  // 1. write
  const int64_t tmp_file_id = 101;
  const int64_t segment_id = 0;
  MacroBlockId macro_id;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id);
  macro_id.set_third_id(segment_id);
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.io_desc_.set_sealed();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_append_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  // check if write through to object storage, and do not write local cache
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);

  // 2. read with is_preread = false
  read_info_.macro_block_id_ = macro_id;
  read_info_.io_desc_.set_no_preread();
  ObStorageObjectHandle read_object_handle;
  ObSSTmpFileReader tmp_file_reader;
  ASSERT_EQ(OB_SUCCESS, tmp_file_reader.aio_read(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_, read_object_handle.get_buffer(), read_info_.size_));

  // 3. wait preread
  usleep(5 * 1000L * 1000L); // sleep 5s

  // 4. check if segment has not been preread to local cache
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_tmp_file_preread_flag.log*");
  OB_LOGGER.set_file_name("test_ss_tmp_file_preread_flag.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
