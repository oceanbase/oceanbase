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
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
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

class TestSSMacroCacheEvictTask : public ::testing::Test
{
public:
  TestSSMacroCacheEvictTask() : write_info_(), read_info_(), write_buf_(), read_buf_() {}
  virtual ~TestSSMacroCacheEvictTask() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void get_macro_cache_used_size(const ObSSMacroCacheType cache_type, int64_t &used_size);

public:
  static const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB
  ObStorageObjectWriteInfo write_info_;
  ObStorageObjectReadInfo read_info_;
  char write_buf_[WRITE_IO_SIZE];
  char read_buf_[WRITE_IO_SIZE];
};

void TestSSMacroCacheEvictTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  file_manager->preread_cache_mgr_.preread_task_.is_inited_ = false;
}

void TestSSMacroCacheEvictTask::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMacroCacheEvictTask::SetUp()
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

void TestSSMacroCacheEvictTask::TearDown()
{
  write_buf_[0] = '\0';
  read_buf_[0] = '\0';
}

void TestSSMacroCacheEvictTask::get_macro_cache_used_size(
    const ObSSMacroCacheType cache_type,
    int64_t &used_size)
{
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  ObSSMacroCacheStat cache_stat;
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(cache_type, cache_stat));
  used_size = cache_stat.used_;
}

TEST_F(TestSSMacroCacheEvictTask, evict_read_cache)
{
  int ret = OB_SUCCESS;

  // 1. write SHARED_MAJOR_DATA_MACRO to object storage
  uint64_t tablet_id = 200;
  uint64_t data_seq = 15;

  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(data_seq); // data_seq
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 2. write SHARED_MAJOR_DATA_MACRO to local cache, which simulates prewarmed SHARED_MAJOR_DATA_MACRO
  write_info_.set_is_write_cache(false); // read cache
  write_info_.set_effective_tablet_id(tablet_id);
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ObSSLocalCacheWriter local_cache_writer;
  ASSERT_EQ(OB_SUCCESS, local_cache_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 3. check macro cache, expect macro cache hit
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_hit_macro_cache = false;
  ObSSFdCacheHandle fd_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_TRUE(is_hit_macro_cache);
  fd_handle.reset();

  // 4. simulate local cache disk space insufficient, so as to trigger macro cache evict
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  const int64_t macro_cache_free_size = disk_space_mgr->get_macro_cache_free_size();
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->alloc_file_size(macro_cache_free_size, ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE));

  // 5. sleep 10s to wait macro cache evict
  sleep(10);

  // 6. check macro cache, expect macro cache miss
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_FALSE(is_hit_macro_cache);
  fd_handle.reset();

  // 7. read and compare, expect read from object storage successfully
  ObLogicMicroBlockId logic_micro_id_1;
  logic_micro_id_1.version_ = ObLogicMicroBlockId::LOGIC_MICRO_ID_VERSION_V1;
  logic_micro_id_1.offset_ = 100;
  logic_micro_id_1.logic_macro_id_.data_seq_.macro_data_seq_ = 1;
  logic_micro_id_1.logic_macro_id_.logic_version_ = 100;
  logic_micro_id_1.logic_macro_id_.tablet_id_ = tablet_id;

  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  read_info_.set_logic_micro_id(logic_micro_id_1);
  read_info_.set_micro_crc(100);
  ObStorageObjectHandle read_object_handle;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));

  ObIOFlag flag;
  ASSERT_EQ(OB_SUCCESS, read_object_handle.get_io_handle().get_io_flag(flag));
  ASSERT_TRUE(flag.is_sync()); // read from object storage
  read_object_handle.reset();

  // 8. release local cache disk size
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->free_file_size(macro_cache_free_size, ObSSMacroCacheType::MACRO_BLOCK,
                                                       ObDiskSpaceType::FILE));
  write_info_.set_is_write_cache(true); // resume default value of is_write_cache flag
}

TEST_F(TestSSMacroCacheEvictTask, evict_tmp_file_write_cache)
{
  int ret = OB_SUCCESS;

  const uint64_t tmp_file_id = 100;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));

  // 1. write local unsealed 8KB tmp file
  // check local cache tmp file free size is enough, so as to ensure write local cache, instead of object storage
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  int64_t macro_cache_free_size = disk_space_mgr->get_macro_cache_free_size();
  ASSERT_LT(8192, macro_cache_free_size);

  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id); // tmp_file_id
  macro_id.set_third_id(1); // segment_id
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.offset_ = 0;
  write_info_.size_ = 8192;
  write_info_.set_tmp_file_valid_length(8192);
  write_info_.io_desc_.set_unsealed();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_append_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  // 2. check macro cache, expect macro cache hit
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_hit_macro_cache = false;
  ObSSFdCacheHandle fd_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_TRUE(is_hit_macro_cache);
  fd_handle.reset();

  // 3. simulate local cache disk space insufficient, so as to trigger macro cache evict
  macro_cache_free_size = disk_space_mgr->get_macro_cache_free_size();
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->alloc_file_size(macro_cache_free_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE));

  // 4. sleep 10s to wait macro cache evict
  sleep(10);

  // 5. check macro cache, expect macro cache miss
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_FALSE(is_hit_macro_cache);
  fd_handle.reset();

  // 6. read and compare the read data with the written data, expect read from object storage successfully
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 0;
  read_info_.size_ = 8192;
  ObStorageObjectHandle read_object_handle;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));

  ObIOFlag flag;
  ASSERT_EQ(OB_SUCCESS, read_object_handle.get_io_handle().get_io_flag(flag));
  ASSERT_TRUE(flag.is_sync()); // read from object storage
  read_object_handle.reset();

  // 7. release local cache disk size
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->free_file_size(macro_cache_free_size, ObSSMacroCacheType::TMP_FILE,
                                                       ObDiskSpaceType::FILE));
}

TEST_F(TestSSMacroCacheEvictTask, evict_other_write_cache)
{
  int ret = OB_SUCCESS;

  uint64_t tablet_id = 100;
  uint64_t server_id = 1;

  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*trasfer_seq*/));

  // 1. write PRIVATE_DATA_MACRO to local cache
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(100); // seq_id
  macro_id.set_macro_transfer_epoch(0); // transfer_seq
  macro_id.set_tenant_seq(server_id);  //tenant_seq
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  // 2. check macro cache, expect macro cache hit
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_hit_macro_cache = false;
  ObSSFdCacheHandle fd_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_TRUE(is_hit_macro_cache);
  fd_handle.reset();

  // 3. simulate local cache disk space insufficient, so as to trigger macro cache evict
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  const int64_t macro_cache_free_size = disk_space_mgr->get_macro_cache_free_size();
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->alloc_file_size(macro_cache_free_size, ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE));

  // 4. sleep 10s to wait macro cache evict
  sleep(10);

  // 5. check macro cache, expect macro cache miss
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_FALSE(is_hit_macro_cache);
  fd_handle.reset();

  // 6. read and compare the read data with the written data, expect read from object storage successfully
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  ObStorageObjectHandle read_object_handle;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));

  ObIOFlag flag;
  ASSERT_EQ(OB_SUCCESS, read_object_handle.get_io_handle().get_io_flag(flag));
  ASSERT_TRUE(flag.is_sync()); // read from object storage
  read_object_handle.reset();

  // 7. release local cache disk size
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->free_file_size(macro_cache_free_size, ObSSMacroCacheType::MACRO_BLOCK,
                                                       ObDiskSpaceType::FILE));
}

TEST_F(TestSSMacroCacheEvictTask, fg_trigger_evict)
{
  int ret = OB_SUCCESS;

  // 1. write SHARED_MAJOR_DATA_MACRO to object storage
  uint64_t tablet_id = 200002;
  uint64_t data_seq = 1;

  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(data_seq); // data_seq
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 2. write SHARED_MAJOR_DATA_MACRO to local cache, which simulates prewarmed SHARED_MAJOR_DATA_MACRO
  write_info_.set_is_write_cache(false); // read cache
  write_info_.set_effective_tablet_id(tablet_id);
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ObSSLocalCacheWriter local_cache_writer;
  ASSERT_EQ(OB_SUCCESS, local_cache_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 3. check macro cache, expect macro cache hit
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_hit_macro_cache = false;
  ObSSFdCacheHandle fd_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_TRUE(is_hit_macro_cache);
  fd_handle.reset();

  // 4. simulate local cache disk space insufficient when alloc_file_size, and trigger macro cache evict
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  const int64_t macro_cache_free_size = disk_space_mgr->get_macro_cache_free_size();
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->alloc_file_size(macro_cache_free_size, ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE));
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, macro_cache_mgr->alloc_file_size(ObSSMacroCacheType::MACRO_BLOCK,
                                                                         4096, ObDiskSpaceType::FILE));

  // 5. sleep 10s to wait macro cache evict
  sleep(10);

  // 6. check macro cache, expect macro cache miss
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_FALSE(is_hit_macro_cache);
  fd_handle.reset();

  // 7. read and compare, expect read from object storage successfully
  ObLogicMicroBlockId logic_micro_id_1;
  logic_micro_id_1.version_ = ObLogicMicroBlockId::LOGIC_MICRO_ID_VERSION_V1;
  logic_micro_id_1.offset_ = 100;
  logic_micro_id_1.logic_macro_id_.data_seq_.macro_data_seq_ = 1;
  logic_micro_id_1.logic_macro_id_.logic_version_ = 100;
  logic_micro_id_1.logic_macro_id_.tablet_id_ = tablet_id;

  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  read_info_.set_logic_micro_id(logic_micro_id_1);
  read_info_.set_micro_crc(100);
  ObStorageObjectHandle read_object_handle;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));

  ObIOFlag flag;
  ASSERT_EQ(OB_SUCCESS, read_object_handle.get_io_handle().get_io_flag(flag));
  ASSERT_TRUE(flag.is_sync()); // read from object storage
  read_object_handle.reset();

  write_info_.set_is_write_cache(true); // resume default value of is_write_cache flag
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_evict_task.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_evict_task.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
