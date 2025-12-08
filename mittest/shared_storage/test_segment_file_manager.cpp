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
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"

#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

class TestSegmentFileManager : public ::testing::Test
{
public:
  TestSegmentFileManager() = default;
  virtual ~TestSegmentFileManager() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
};

void TestSegmentFileManager::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestSegmentFileManager::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestSegmentFileManager, test_gc_segment_file)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  const int64_t tmp_file_id = 30;
  const int64_t segment_id = 40;
  const int64_t valid_length = 8 * 1024;
  file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE));
  file_id.set_second_id(tmp_file_id);  //tmp_file_id
  file_id.set_third_id(segment_id);   //segment_id
  file_id.set_fourth_id(valid_length);   //valid_length

  // step 1: test write remote segment file
  TmpFileSegId seg_id(tmp_file_id, segment_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  const int64_t write_io_size = valid_length; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_tmp_file_info;
  write_tmp_file_info.buffer_ = write_buf;
  write_tmp_file_info.offset_ = 0;
  write_tmp_file_info.size_ = write_io_size;
  write_tmp_file_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
  write_tmp_file_info.io_desc_.set_unsealed();
  write_tmp_file_info.mtl_tenant_id_ = MTL_ID();
  write_tmp_file_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(write_tmp_file_info.mtl_tenant_id_);
  write_tmp_file_info.set_tmp_file_valid_length(write_io_size);
  ObSSObjectStorageWriter object_storage_writer;
  ASSERT_EQ(OB_SUCCESS, object_storage_writer.aio_write(write_tmp_file_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  // step 2: test gc remote segment file
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_segment_file_mgr().push_seg_file_to_remove_queue(seg_id, valid_length));
  ob_usleep(11*1000*1000L); // 11s
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(file_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);

  // step 5: test delete file
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(file_id));
}

TEST_F(TestSegmentFileManager, test_overwrite_segment_file)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager*);
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ASSERT_NE(nullptr, disk_space_mgr);
  MacroBlockId file_id;
  const int64_t tmp_file_id = 30;
  const int64_t segment_id = 41;
  file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  file_id.set_second_id(tmp_file_id);  //tmp_file_id
  file_id.set_third_id(segment_id);   //segment_id

  // test 1: write local segment file 0-16KB, alloc_size=16KB
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  const int64_t write_io_size = 16 * 1024; // 16KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_tmp_file_valid_length(write_io_size);

  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  int64_t expected_tmp_file_alloc_size = write_io_size;
  expected_tmp_file_alloc_size += ObDirManager::DEFAULT_DIR_SIZE;
  expected_tmp_file_alloc_size += ObDirManager::DEFAULT_DIR_SIZE; // scatter_id_dir
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  ObSSMacroCacheStat cache_stat;
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  ASSERT_EQ(expected_tmp_file_alloc_size, cache_stat.used_);
  write_object_handle.reset();

  // test 2: write local segment file 0-16KB, alloc_size=0
  write_info.set_tmp_file_valid_length(write_io_size);
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  ASSERT_EQ(expected_tmp_file_alloc_size, cache_stat.used_);
  write_object_handle.reset();

  // test 3: write local segment file 8KB-24KB, alloc_size=8KB
  write_info.offset_ = 8 * 1024; // 8KB
  write_info.set_tmp_file_valid_length(24 * 1024);  // 24KB
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  expected_tmp_file_alloc_size += 8 * 1024;
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  ASSERT_EQ(expected_tmp_file_alloc_size, cache_stat.used_);
  write_object_handle.reset();

  // test 4: write local segment file 32KB-48KB (with 8KB hole), alloc_size=24KB
  write_info.offset_ = 24 * 1024 + 8 * 1024; // 32KB
  write_info.set_tmp_file_valid_length(48 * 1024); // 48KB
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  expected_tmp_file_alloc_size += 24 * 1024;
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  ASSERT_EQ(expected_tmp_file_alloc_size, cache_stat.used_);
  write_object_handle.reset();

  // test 5: write local segment file 48KB-64KB, alloc_size=16KB
  write_info.offset_ = 48 * 1024; // 24KB
  write_info.set_tmp_file_valid_length(64 * 1024);  // 64KB
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  expected_tmp_file_alloc_size += 16 * 1024;
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  ASSERT_EQ(expected_tmp_file_alloc_size, cache_stat.used_);
  write_object_handle.reset();

  // test 6: write one new local segment file 8KB-16KB(with 8KB hole), alloc_size=16KB
  file_id.set_third_id(segment_id + 1);   //segment_id
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  write_info.offset_ = 8 * 1024; // 8KB
  write_info.size_ = 8 * 1024; // 8KB
  write_info.set_tmp_file_valid_length(16 * 1024); // 16KB
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  expected_tmp_file_alloc_size += 16 * 1024; // 16KB
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  ASSERT_EQ(expected_tmp_file_alloc_size, cache_stat.used_);
  write_object_handle.reset();

  // test 7: delete tmp file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(file_id));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_segment_file_manager.log*");
  OB_LOGGER.set_file_name("test_segment_file_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
