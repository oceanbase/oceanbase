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
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/shared_storage/ob_segment_file_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_tmp_file_io_callback.h"
#include <thread>
#include <atomic>
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;

class TestSSReaderWriter : public ::testing::Test
{
public:
  class TestIOCallback : public ObIOCallback
  {
  public:
    TestIOCallback()
      : ObIOCallback(ObIOCallbackType::TEST_CALLBACK), number_(nullptr), allocator_(nullptr),
        help_buf_(nullptr)
    {
    }
    virtual ~TestIOCallback()
    {
      if (nullptr != number_) {
        *number_ -= 90;
        number_ = nullptr;
      }
      if (nullptr != allocator_) {
        if (nullptr != help_buf_) {
          allocator_->free(help_buf_);
          help_buf_ = nullptr;
        }
        allocator_->free(this);
        LOG_INFO("success reset callback when out_rec_cnt = 0");
      }
    }

    virtual const char *get_data() override { return (char *)help_buf_; }
    virtual int64_t size() const override { return sizeof(TestIOCallback); }
    virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(allocator_)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("Invalid data, the allocator is NULL, ", K(ret));
      } else if (OB_UNLIKELY(data_size <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid data buffer size", K(ret), K(data_size));
      } else if (OB_ISNULL(help_buf_ = static_cast<char *>(allocator_->alloc(data_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate help buf", K(ret), K(data_size), KP(help_buf_));
      } else {
        memset(help_buf_, 0, data_size);
        MEMCPY(help_buf_, io_data_buffer, data_size);
      }
      return OB_SUCCESS;
    }
    virtual int inner_process(const char *data_buffer, const int64_t size) override
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(allocator_)) {
        // for test, ignore
      } else if (OB_FAIL(alloc_data_buf(data_buffer, size))) {
        LOG_WARN("Fail to allocate memory, ", K(ret), K(size));
      }
      if (nullptr != number_) {
        *number_ += 100;
      }
      return OB_SUCCESS;
    }
    virtual ObIAllocator *get_allocator() override { return allocator_; }
    const char *get_cb_name() const override { return "TestIOCallback"; }
    TO_STRING_KV(KP(number_), KP(allocator_), KP(help_buf_));

  public:
    int64_t *number_;
    ObIAllocator *allocator_;
    char *help_buf_;
  };

  TestSSReaderWriter() : write_info_(), read_info_(), write_buf_(), read_buf_() {}
  virtual ~TestSSReaderWriter() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void exhaust_tmp_file_disk_size(int64_t &avail_size);
  void alloc_tmp_file_disk_size(const int64_t disk_size);
  void release_tmp_file_disk_size(const int64_t avail_size);
  void check_tmp_file_disk_size_enough(const int64_t size);
  void write_tmp_file_data(const MacroBlockId &macro_id,
                           const int64_t offset,
                           const int64_t size,
                           const int64_t valid_length,
                           const bool is_sealed,
                           const char *buffer);
  void read_and_compare_tmp_file_data(const MacroBlockId &macro_id,
                                      const int64_t offset,
                                      const int64_t size);
  void check_tmp_file_seg_meta(const MacroBlockId &macro_id,
                               const bool is_meta_exist,
                               const bool is_in_local = false,
                               const int64_t valid_length = 0);

public:
  static const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 768; // 3MB
  ObStorageObjectWriteInfo write_info_;
  ObStorageObjectReadInfo read_info_;
  char write_buf_[WRITE_IO_SIZE];
  char read_buf_[WRITE_IO_SIZE];
};

void TestSSReaderWriter::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestSSReaderWriter::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSReaderWriter::SetUp()
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

void TestSSReaderWriter::TearDown()
{
  // Ensure disk space is released for next test
  ObTenantDiskSpaceManager *disk_space_manager = MTL(ObTenantDiskSpaceManager *);
  if (OB_NOT_NULL(disk_space_manager)) {
    // Try to release a large amount of disk space to reset state
    // This handles cases where tests exhaust disk space but fail before cleanup
    int64_t large_size = 100 * 1024 * 1024; // 100MB
    disk_space_manager->free_file_size(large_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE);
    disk_space_manager->free_file_size(large_size, ObSSMacroCacheType::META_FILE, ObDiskSpaceType::FILE);
    disk_space_manager->free_file_size(large_size, ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE);
    LOG_INFO("TearDown: attempted to release disk space for next test");
  }

  write_buf_[0] = '\0';
  read_buf_[0] = '\0';
}

void TestSSReaderWriter::exhaust_tmp_file_disk_size(int64_t &avail_size)
{
  static int64_t call_times = 0;
  call_times++;
  ObTenantDiskSpaceManager *disk_space_manager = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_manager) << "call_times: " << call_times;
  avail_size = disk_space_manager->get_macro_cache_free_size();
  ASSERT_EQ(OB_SUCCESS, disk_space_manager->alloc_file_size(avail_size,
            ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE)) << "call_times: " << call_times;
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, disk_space_manager->alloc_file_size(8192,
            ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE)) << "call_times: " << call_times;
}

void TestSSReaderWriter::alloc_tmp_file_disk_size(const int64_t disk_size)
{
  static int64_t call_times = 0;
  call_times++;
  ObTenantDiskSpaceManager *disk_space_manager = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_manager) << "call_times: " << call_times;
  ASSERT_EQ(OB_SUCCESS, disk_space_manager->alloc_file_size(disk_size,
            ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE)) << "call_times: " << call_times;
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, disk_space_manager->alloc_file_size(8192,
            ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE)) << "call_times: " << call_times;
}

void TestSSReaderWriter::release_tmp_file_disk_size(const int64_t avail_size)
{
  static int64_t call_times = 0;
  call_times++;
  ObTenantDiskSpaceManager *disk_space_manager = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_manager) << "call_times: " << call_times;
  ASSERT_EQ(OB_SUCCESS, disk_space_manager->free_file_size(avail_size,
            ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE)) << "call_times: " << call_times;
}

void TestSSReaderWriter::check_tmp_file_disk_size_enough(const int64_t size)
{
  static int64_t call_times = 0;
  call_times++;
  ObTenantDiskSpaceManager *disk_space_manager = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_manager) << "call_times: " << call_times;
  ASSERT_LT(size, disk_space_manager->get_macro_cache_free_size()) << "call_times: " << call_times;
}

void TestSSReaderWriter::write_tmp_file_data(
    const MacroBlockId &macro_id,
    const int64_t offset,
    const int64_t size,
    const int64_t valid_length,
    const bool is_sealed,
    const char *buffer)
{
  static int64_t call_times = 0;
  call_times++;
  ObStorageObjectHandle write_object_handle;
  ASSERT_TRUE(macro_id.is_valid()) << "call_times: " << call_times;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id)) << "call_times: " << call_times;
  write_info_.offset_ = offset;
  write_info_.size_ = size;
  write_info_.set_tmp_file_valid_length(valid_length);
  if (is_sealed) {
    write_info_.io_desc_.set_sealed();
  } else {
    write_info_.io_desc_.set_unsealed();
  }
  write_info_.buffer_ = buffer;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_append_file(write_info_, write_object_handle)) << "call_times: " << call_times;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait()) << "call_times: " << call_times;
}

void TestSSReaderWriter::read_and_compare_tmp_file_data(
    const MacroBlockId &macro_id,
    const int64_t offset,
    const int64_t size)
{
  static int64_t call_times = 0;
  call_times++;
  ObStorageObjectHandle read_object_handle;
  ObSSTmpFileReader tmp_file_reader;
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = offset;
  read_info_.size_ = size;
  ASSERT_EQ(OB_SUCCESS, tmp_file_reader.aio_read(read_info_, read_object_handle)) << "call_times: " << call_times;
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait()) << "call_times: " << call_times;
  ASSERT_NE(nullptr, read_object_handle.get_buffer()) << "call_times: " << call_times;
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size()) << "call_times: " << call_times;
  ASSERT_EQ(0, memcmp(write_buf_ + offset, read_object_handle.get_buffer(), size)) << "call_times: " << call_times;
  memset(read_buf_, 0, WRITE_IO_SIZE);
}

void TestSSReaderWriter::check_tmp_file_seg_meta(
    const MacroBlockId &macro_id,
    const bool is_meta_exist,
    const bool is_in_local,
    const int64_t valid_length)
{
  static int64_t call_times = 0;
  call_times++;
  TmpFileSegId seg_id(macro_id.second_id(), macro_id.third_id());
  TmpFileMetaHandle meta_handle;
  bool exist = false;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager) << "call_times: " << call_times;
  ASSERT_EQ(OB_SUCCESS, file_manager->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, exist)) << "call_times: " << call_times;
  ASSERT_EQ(is_meta_exist, exist) << "call_times: " << call_times;
  if (is_meta_exist) {
    ASSERT_TRUE(meta_handle.is_valid());
    ASSERT_TRUE(meta_handle.get_tmpfile_meta()->is_valid());
    ASSERT_EQ(is_in_local, meta_handle.is_in_local()) << "call_times: " << call_times;
    ASSERT_EQ(valid_length, meta_handle.get_valid_length()) << "call_times: " << call_times;
  }
}

TEST_F(TestSSReaderWriter, local_cache_reader_writer)
{
  int ret = OB_SUCCESS;

  uint64_t ls_id = 1;
  uint64_t ls_epoch_id = 1;
  uint64_t tablet_id = 800;
  int64_t transfer_seq = 0;

  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_ls_id_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_meta_tablet_id_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_meta_tablet_id_private_transfer_epoch_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id, transfer_seq));

  // 1. write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TENANT_DISK_SPACE_META);
  macro_id.set_second_id(MTL_ID());
  macro_id.set_third_id(MTL_EPOCH_ID());
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.set_ls_epoch_id(ls_epoch_id);

  ObSSLocalCacheWriter local_cache_writer;
  ASSERT_EQ(OB_SUCCESS, local_cache_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  // 2. read and compare the read data with the written data
  read_info_.macro_block_id_ = macro_id;
  read_info_.set_ls_epoch_id(ls_epoch_id);
  ObStorageObjectHandle read_object_handle;
  ObSSLocalCacheReader local_cache_reader;
  ASSERT_EQ(OB_SUCCESS, local_cache_reader.aio_read(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_, read_object_handle.get_buffer(), WRITE_IO_SIZE));
  read_object_handle.reset();

  // 3. try read DATA_OUT_OF_RANGE, expect io layer atomically adjust read size and read successfully
  read_info_.macro_block_id_ = macro_id;
  read_info_.set_ls_epoch_id(ls_epoch_id);
  read_info_.offset_ = 10000;
  read_info_.size_ = WRITE_IO_SIZE;
  ASSERT_EQ(OB_SUCCESS, local_cache_reader.aio_read(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_ - read_info_.offset_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(),
                      read_info_.size_ - read_info_.offset_));
  read_object_handle.reset();

  // 4. try read offset exceeds acutal file length, expect OB_DATA_OUT_OF_RANGE errno
  read_info_.macro_block_id_ = macro_id;
  read_info_.set_ls_epoch_id(ls_epoch_id);
  read_info_.offset_ = WRITE_IO_SIZE;
  read_info_.size_ = 1;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, local_cache_reader.aio_read(read_info_, read_object_handle));
  read_object_handle.reset();
}

void check_local_cache_tablet_stat(const common::ObTabletID effective_tablet_id,
                                   const int64_t access_size,
                                   const int64_t access_cnt,
                                   const int64_t hit_size,
                                   const int64_t hit_cnt)
{
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ObStorageCacheHitStat entry;
  local_cache_service->get_local_cache_tablet_stat(effective_tablet_id, entry);
  ASSERT_EQ(access_cnt, entry.get_miss_cnt() + entry.get_hit_cnt());
  ASSERT_EQ(access_size, entry.get_miss_bytes() + entry.get_hit_bytes());
  ASSERT_EQ(hit_cnt, entry.get_hit_cnt());
  ASSERT_EQ(hit_size, entry.get_hit_bytes());
}
void check_object_type_stat(const MacroBlockId &macro_id,
                          const uint64_t read_cnt,
                          const uint64_t read_size,
                          const uint64_t write_cnt,
                          const uint64_t write_size,
                          ObStorageObjectHandle &object_handle)
{
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ObSSObjectTypeStat type_stat;
  ObIOFlag flag;
  object_handle.get_io_handle().get_io_flag(flag);
  bool is_remote = flag.is_sync();
  ASSERT_EQ(OB_SUCCESS, local_cache_service->get_object_type_stat(macro_id.storage_object_type(), is_remote, type_stat));
  ObSSBaseStat stat;
  type_stat.get_stat(ObSSObjectTypeStatType::READ, stat);
  ASSERT_EQ(read_cnt, stat.get_cnt());
  ASSERT_EQ(read_size, stat.get_size());
  type_stat.get_stat(ObSSObjectTypeStatType::WRITE, stat);
  ASSERT_EQ(write_cnt, stat.get_cnt());
  ASSERT_EQ(write_size, stat.get_size());
}

TEST_F(TestSSReaderWriter, private_macro_reader_writer)
{
  int ret = OB_SUCCESS;

  uint64_t tablet_id = 100;
  uint64_t server_id = 1;
  int64_t access_size = 0;
  int64_t access_cnt = 0;
  int64_t hit_size = 0;
  int64_t hit_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_private_transfer_epoch_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*trasfer_seq*/));

  // 1. write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(100); // seq_id
  macro_id.set_macro_private_transfer_epoch(0); // transfer_seq
  macro_id.set_tenant_seq(server_id);  //tenant_seq
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ObSSPrivateMacroWriter private_macro_writer;
  ASSERT_EQ(OB_SUCCESS, private_macro_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  check_object_type_stat(macro_id, 0/*read_cnt*/, 0/*read_size*/, 1/*write_cnt*/, write_info_.size_, write_object_handle);
  // 2. check macro cache
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_hit_macro_cache = false;
  ObSSFdCacheHandle fd_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_TRUE(is_hit_macro_cache);

  // 3. read and compare the read data with the written data
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  read_info_.set_effective_tablet_id(ObTabletID(tablet_id));
  ObStorageObjectHandle read_object_handle;
  ObSSPrivateMacroReader private_macro_reader;
  ASSERT_EQ(OB_SUCCESS, private_macro_reader.aio_read(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));

  // 4. check local cache tablet stat hit and first insert
  access_size += read_info_.size_;
  access_cnt++;
  hit_size += read_info_.size_;
  hit_cnt++;
  check_local_cache_tablet_stat(read_info_.get_effective_tablet_id(), access_size, access_cnt, hit_size, hit_cnt);
  // 5. check local cache tablet stat hit and update
  ASSERT_EQ(OB_SUCCESS, private_macro_reader.aio_read(read_info_, read_object_handle));
  access_size += read_info_.size_;
  access_cnt++;
  hit_size += read_info_.size_;
  hit_cnt++;
  check_local_cache_tablet_stat(read_info_.get_effective_tablet_id(), access_size, access_cnt, hit_size, hit_cnt);
  ASSERT_EQ(OB_SUCCESS, private_macro_reader.aio_read(read_info_, read_object_handle));
  read_object_handle.reset();
}

TEST_F(TestSSReaderWriter, share_macro_reader_writer)
{
  int ret = OB_SUCCESS;

  uint64_t tablet_id = 200;
  uint64_t data_seq = 15;

  // 1. write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(data_seq); // data_seq
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ObSSShareMacroWriter share_macro_writer;
  ASSERT_EQ(OB_SUCCESS, share_macro_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  check_object_type_stat(macro_id, 0/*read_cnt*/, 0/*read_size*/, 1/*write_cnt*/, write_info_.size_, write_object_handle);

  ObLogicMicroBlockId logic_micro_id_1;
  logic_micro_id_1.version_ = ObLogicMicroBlockId::LOGIC_MICRO_ID_VERSION_V1;
  logic_micro_id_1.offset_ = 100;
  logic_micro_id_1.logic_macro_id_.data_seq_.macro_data_seq_ = 1;
  logic_micro_id_1.logic_macro_id_.logic_version_ = 100;
  logic_micro_id_1.logic_macro_id_.tablet_id_ = tablet_id;

  ObLogicMicroBlockId logic_micro_id_2;
  logic_micro_id_2 = logic_micro_id_1;
  logic_micro_id_2.offset_ = 1;
  int64_t access_size = 0;
  int64_t access_cnt = 0;
  int64_t hit_size = 0;
  int64_t hit_cnt = 0;
  // 2. read <1, WRITE_IO_SIZE / 2>, expect cache and load cache
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  read_info_.set_logic_micro_id(logic_micro_id_1);
  read_info_.set_micro_crc(100);
  read_info_.set_effective_tablet_id(ObTabletID(tablet_id));
  ObStorageObjectHandle cache_miss_read_object_handle;
  ObSSShareMacroReader share_macro_reader;
  ASSERT_EQ(OB_SUCCESS, share_macro_reader.aio_read(read_info_, cache_miss_read_object_handle));
  ASSERT_EQ(OB_SUCCESS, cache_miss_read_object_handle.wait());
  ASSERT_NE(nullptr, cache_miss_read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, cache_miss_read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, cache_miss_read_object_handle.get_buffer(), read_info_.size_));
  cache_miss_read_object_handle.reset();
  // check local cache tablet stat no hit and first insert
  access_size += read_info_.size_;
  access_cnt++;
  check_local_cache_tablet_stat(read_info_.get_effective_tablet_id(), access_size, access_cnt, hit_size, hit_cnt);
  // 3. read <1, WRITE_IO_SIZE / 2>, expect hit memory
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  read_info_.set_logic_micro_id(logic_micro_id_1);
  read_info_.set_micro_crc(100);
  read_info_.set_effective_tablet_id(ObTabletID(tablet_id));
  ObStorageObjectHandle hit_memory_read_object_handle;
  ASSERT_EQ(OB_SUCCESS, share_macro_reader.aio_read(read_info_, hit_memory_read_object_handle));
  ASSERT_EQ(OB_SUCCESS, hit_memory_read_object_handle.wait());
  ASSERT_NE(nullptr, hit_memory_read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, hit_memory_read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, hit_memory_read_object_handle.get_buffer(), read_info_.size_));
  hit_memory_read_object_handle.reset();
  // check local cache tablet stat hit and update
  access_size += read_info_.size_;
  access_cnt++;
  hit_size += read_info_.size_;
  hit_cnt++;
  check_local_cache_tablet_stat(read_info_.get_effective_tablet_id(), access_size, access_cnt, hit_size, hit_cnt);
  // 4. read <WRITE_IO_SIZE / 2, WRITE_IO_SIZE>, expect cache miss and load cache
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = WRITE_IO_SIZE / 2;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  read_info_.set_logic_micro_id(logic_micro_id_2);
  read_info_.set_micro_crc(200);
  read_info_.set_effective_tablet_id(ObTabletID(tablet_id));
  ASSERT_EQ(OB_SUCCESS, share_macro_reader.aio_read(read_info_, cache_miss_read_object_handle));
  ASSERT_EQ(OB_SUCCESS, cache_miss_read_object_handle.wait());
  ASSERT_NE(nullptr, cache_miss_read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, cache_miss_read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, cache_miss_read_object_handle.get_buffer(), read_info_.size_));
  cache_miss_read_object_handle.reset();
  // check local cache tablet stat no hit and update
  access_size += read_info_.size_;
  access_cnt++;
  check_local_cache_tablet_stat(read_info_.get_effective_tablet_id(), access_size, access_cnt, hit_size, hit_cnt);
  // 5. wait <1, WRITE_IO_SIZE / 2> flush from memory to disk
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

  // 6. read <1, WRITE_IO_SIZE / 2>, expect hit disk
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  read_info_.set_logic_micro_id(logic_micro_id_1);
  read_info_.set_micro_crc(100);
  read_info_.set_effective_tablet_id(ObTabletID(tablet_id));
  ObStorageObjectHandle hit_disk_read_object_handle;
  ASSERT_EQ(OB_SUCCESS, share_macro_reader.aio_read(read_info_, hit_disk_read_object_handle));
  ASSERT_EQ(OB_SUCCESS, hit_disk_read_object_handle.wait());
  ASSERT_NE(nullptr, hit_disk_read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, hit_disk_read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, hit_disk_read_object_handle.get_buffer(), read_info_.size_));
  hit_disk_read_object_handle.reset();
  // check local cache tablet stat hit and update
  access_size += read_info_.size_;
  access_cnt++;
  hit_size += read_info_.size_;
  hit_cnt++;
  check_local_cache_tablet_stat(read_info_.get_effective_tablet_id(), access_size, access_cnt, hit_size, hit_cnt);
}

// Test parent directory creation failure with three fallback scenarios
TEST_F(TestSSReaderWriter, test_file_alloc_success_but_parent_dir_fail)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  // to avoid affecting tmp file seg_meta_map and tmp file tmp_file_write_free_disk_size
  // disable tmp_file_flush_task, preread_task_, calibrate_disk_space_task and gc_unsealed_tmp_file_task
  // preread_task_ will affect local disk size, so preread_task_ need to disble
  file_manager->preread_cache_mgr_.preread_task_.is_inited_ = false;
  macro_cache_mgr->evict_task_.is_inited_ = false;
  macro_cache_mgr->flush_task_.is_inited_ = false;
  file_manager->calibrate_disk_space_task_.is_inited_ = false;
  file_manager->segment_file_mgr_.gc_segment_file_task_.is_inited_ = false;
  sleep(3);

  const int64_t write_size = 8192;

  // Case 1: Meta does not exist - should use write_through_on_meta_not_exist
  {
    LOG_INFO("=== Case 1: test_fallback_meta_not_exist ===");
    uint64_t tmp_file_id = 601;
    MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(tmp_file_id);
    macro_id.set_third_id(1);  // segment_id
    ASSERT_TRUE(macro_id.is_valid());

    // Inject error to simulate parent dir creation failure for new segment
    TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_DIR_DISK_OUTOF_SPACE_ERR, OB_SERVER_OUTOF_DISK_SPACE, 0, 1);

    // Execute write - should fallback to write_through_on_meta_not_exist
    write_tmp_file_data(macro_id, 0/*offset*/, write_size, write_size/*valid_length*/, false/*is_sealed*/, write_buf_);
    LOG_INFO("async append file completed with meta_not_exist fallback", K(macro_id));

    // Disable error injection
    TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_DIR_DISK_OUTOF_SPACE_ERR, OB_SUCCESS, 0, 0);

    // Verify
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, write_size/*size*/);
    check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, write_size/*valid_length*/);
    LOG_INFO("=== Case 1 completed successfully ===");
  }

  // Case 2: Meta exists + append sealed - should use write_through_with_remote_seg
  {
    LOG_INFO("=== Case 2: test_fallback_meta_exist_sealed ===");
    uint64_t tmp_file_id = 602;

    // 1. First write: create initial segment in object storage (disk exhausted)
    int64_t avail_size = 0;
    exhaust_tmp_file_disk_size(avail_size);

    MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(tmp_file_id);
    macro_id.set_third_id(2);  // segment_id
    ASSERT_TRUE(macro_id.is_valid());

    // Write initial segment to object storage (no parent dir created since disk exhausted)
    write_tmp_file_data(macro_id, 0/*offset*/, write_size, write_size/*valid_length*/, false/*is_sealed*/, write_buf_);
    check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, write_size/*valid_length*/);

    // 2. Second write: append sealed segment with parent dir failure

    // Release disk space for allocation but inject parent dir creation failure
    release_tmp_file_disk_size(avail_size);
    check_tmp_file_disk_size_enough(write_size * 2);

    // Inject error to simulate parent dir creation failure
    TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_DIR_DISK_OUTOF_SPACE_ERR, OB_SERVER_OUTOF_DISK_SPACE, 0, 1);

    // Append sealed segment - should fallback
    write_tmp_file_data(macro_id, write_size/*offset*/, write_size, write_size * 2/*valid_length*/, true/*is_sealed*/, write_buf_);
    LOG_INFO("async append sealed file completed with meta_exist fallback", K(macro_id));

    // Disable error injection
    TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_DIR_DISK_OUTOF_SPACE_ERR, OB_SUCCESS, 0, 0);

    // Verify
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, write_size * 2/*size*/);
    check_tmp_file_seg_meta(macro_id, false/*is_meta_exist*/);
    LOG_INFO("=== Case 2 completed successfully ===");
  }

  // Case 3: Meta exists + append unsealed - should use write_through_with_remote_seg
  {
    LOG_INFO("=== Case 3: test_fallback_meta_exist_unsealed ===");
    uint64_t tmp_file_id = 603;

    // 1. First write: create initial segment in object storage (disk exhausted)
    int64_t avail_size = 0;
    exhaust_tmp_file_disk_size(avail_size);

    MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(tmp_file_id);
    macro_id.set_third_id(3);  // segment_id
    ASSERT_TRUE(macro_id.is_valid());

    // Write initial segment to object storage (no parent dir created since disk exhausted)
    write_tmp_file_data(macro_id, 0/*offset*/, write_size, write_size/*valid_length*/, false/*is_sealed*/, write_buf_);
    check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, write_size/*valid_length*/);

    // 2. Second write: append unsealed segment with parent dir failure

    // Release disk space for allocation but inject parent dir creation failure
    release_tmp_file_disk_size(avail_size);
    check_tmp_file_disk_size_enough(write_size * 2);

    // Inject error to simulate parent dir creation failure
    TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_DIR_DISK_OUTOF_SPACE_ERR, OB_SERVER_OUTOF_DISK_SPACE, 0, 1);

    // Append unsealed segment - should fallback
    write_tmp_file_data(macro_id, write_size/*offset*/, write_size, write_size * 2/*valid_length*/, false/*is_sealed*/, write_buf_);
    LOG_INFO("async append unsealed file completed with meta_exist fallback", K(macro_id));

    // Disable error injection
    TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_DIR_DISK_OUTOF_SPACE_ERR, OB_SUCCESS, 0, 0);

    // Verify
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, write_size * 2/*size*/);
    check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, write_size * 2/*valid_length*/);
    LOG_INFO("=== Case 3 completed successfully ===");
  }
}

TEST_F(TestSSReaderWriter, tmp_file_reader_writer)
{
  int ret = OB_SUCCESS;

  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  // to avoid affecting tmp file seg_meta_map and tmp file tmp_file_write_free_disk_size
  // disable tmp_file_flush_task, preread_task_, calibrate_disk_space_task and gc_unsealed_tmp_file_task
  // preread_task_ will affect local disk size, so preread_task_ need to disble
  file_manager->preread_cache_mgr_.preread_task_.is_inited_ = false;
  macro_cache_mgr->evict_task_.is_inited_ = false;
  macro_cache_mgr->flush_task_.is_inited_ = false;
  file_manager->calibrate_disk_space_task_.is_inited_ = false;
  file_manager->segment_file_mgr_.gc_segment_file_task_.is_inited_ = false;
  sleep(3);

  uint64_t tmp_file_id = 100;

  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));

  // 1.1 disk space enough, write local, write one new unsealed segment with offset = 0
  check_tmp_file_disk_size_enough(8192);
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id); // tmp_file_id
  macro_id.set_third_id(10); // segment_id
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192/*valid_length*/);

  // 1.2 write another two unsealed local seg (seg_id: 11, 12), for append test case below
  for (int64_t i = 1; i <= 2; ++i) {
    check_tmp_file_disk_size_enough(8192);
    macro_id.set_third_id(10 + i); // segment_id
    write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);
    check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192/*valid_length*/);
  }

  // 2. disk space enough, write local, write one new sealed segment with offset = 0
  check_tmp_file_disk_size_enough(8192);
  macro_id.set_third_id(20); // segment_id
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, true/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192/*valid_length*/);

  // 3. write one new unsealed segment with offset > 0
  macro_id.set_third_id(30); // segment_id
  ASSERT_TRUE(macro_id.is_valid());
  write_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/, 8192 * 2/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192 * 2/*valid_length*/);

  // 4.1 disk space not enough, write through, write one new unsealed segment with offset = 0
  int64_t avail_size = 0;
  exhaust_tmp_file_disk_size(avail_size);

  macro_id.set_third_id(40); // segment_id
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192/*valid_length*/);

  // 4.2 write another four unsealed remote seg (seg_id: 41, 42, 43, 44), for append test case below
  for (int64_t i = 1; i <= 4; ++i) {
    macro_id.set_third_id(40 + i); // segment_id
    write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);
    check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192/*valid_length*/);
  }

  // 5. disk space not enough, write through, write one new sealed segment with offset = 0
  macro_id.set_third_id(50); // segment_id
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, true/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, false/*is_meta_exist*/);

  // 6. seg meta already exist, io.valid_len <= seg_meta.valid_len. no need write io and simulate io result
  macro_id.set_third_id(60); // segment_id
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, true/*is_sealed*/, write_buf_);

  // 7. local seg meta already exist, io.valid_len > seg_meta.valid_len.
  // disk space enough, write local, append unsealed segment
  release_tmp_file_disk_size(avail_size);
  check_tmp_file_disk_size_enough(8192);

  macro_id.set_third_id(10); // segment_id
  write_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/, 8192 * 2/*valid_length*/, false/*is_sealed*/, write_buf_ + 8192);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192 * 2/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192 * 2/*valid_length*/);

  // 8. local seg meta already exist, io.valid_len > seg_meta.valid_len.
  // disk space enough, write local, append sealed segment
  check_tmp_file_disk_size_enough(8192);

  macro_id.set_third_id(10); // segment_id
  write_tmp_file_data(macro_id, 8192 * 2/*offset*/, 8192/*size*/, 8192 * 3/*valid_length*/, true/*is_sealed*/, write_buf_ + (8192 * 2));
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192 * 3/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192 * 3/*valid_length*/);

  // 9. local seg meta already exist, io.valid_len > seg_meta.valid_len.
  // disk space not enough, write through, append unsealed segment
  exhaust_tmp_file_disk_size(avail_size);

  macro_id.set_third_id(11); // segment_id
  write_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/, 8192 * 2/*valid_length*/, false/*is_sealed*/, write_buf_ + 8192);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192 * 2/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192 * 2/*valid_length*/);

  // 10. local seg meta already exist, io.valid_len > seg_meta.valid_len.
  // disk space not enough, write through, append sealed segment
  // append write of segment 11 will delete local old segment 11, which free tmp_file_alloc_size
  alloc_tmp_file_disk_size(8192);

  macro_id.set_third_id(12); // segment_id
  write_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/, 8192 * 2/*valid_length*/, true/*is_sealed*/, write_buf_ + 8192);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192 * 2/*size*/);
  check_tmp_file_seg_meta(macro_id, false/*is_meta_exist*/);

  // 11. remote seg meta already exist, io.valid_len > seg_meta.valid_len.
  // disk space enough, write local, append unsealed segment
  release_tmp_file_disk_size(avail_size + 8192);
  check_tmp_file_disk_size_enough(8192);

  macro_id.set_third_id(41); // segment_id
  write_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/, 8192 * 2/*valid_length*/, false/*is_sealed*/, write_buf_ + 8192);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192 * 2/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192 * 2/*valid_length*/);

  // 12. remote seg meta already exist, io.valid_len > seg_meta.valid_len.
  // disk space enough, append sealed segment, write remote
  check_tmp_file_disk_size_enough(8192);
  macro_id.set_third_id(42); // segment_id
  write_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/, 8192 * 2/*valid_length*/, true/*is_sealed*/, write_buf_ + 8192);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192 * 2/*size*/);
  check_tmp_file_seg_meta(macro_id, false/*is_meta_exist*/);

  // 13. remote seg meta already exist, io.valid_len > seg_meta.valid_len.
  // disk space not enough, write remote, append unsealed segment
  exhaust_tmp_file_disk_size(avail_size);

  macro_id.set_third_id(43); // segment_id
  write_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/, 8192 * 2/*valid_length*/, false/*is_sealed*/, write_buf_ + 8192);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192 * 2/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192 * 2/*valid_length*/);

  // 14. remote seg meta already exist, io.valid_len > seg_meta.valid_len.
  // disk space not enough, write remote, append sealed segment
  macro_id.set_third_id(44); // segment_id
  write_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/, 8192 * 2/*valid_length*/, true/*is_sealed*/, write_buf_ + 8192);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192 * 2/*size*/);
  check_tmp_file_seg_meta(macro_id, false/*is_meta_exist*/);

  // 15. (a) disk space not enough, write remote, unsealed, [0, 8KB);
  //     (b) disk space not enough, write remote, sealed, [0, 2MB);
  macro_id.set_third_id(71); // segment_id
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192/*valid_length*/);

  write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);
  check_tmp_file_seg_meta(macro_id, false/*is_meta_exist*/);


  // 16. concurrent append and calibration
  // (a) disk space enough, write local, unsealed, [0, 8KB);
  // (b) pause tmp file gc;
  // (c) append sealed segment, [0, 2MB], write remote; expect write local and not delete tmp_file_seg_meta
  release_tmp_file_disk_size(avail_size);
  check_tmp_file_disk_size_enough(8192);
  macro_id.set_third_id(72); // segment_id
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192/*valid_length*/);

  file_manager->set_tmp_file_cache_pause_gc();

  write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/);

  file_manager->set_tmp_file_cache_allow_gc();

  // 17. concurrent append and calibration
  // (a) disk space enough, write local, unsealed;
  // (b) pause tmp file gc;
  // (c) disk space not enough, write remote, append unsealed segment
  // (d) rm_logical_deleted_file
  check_tmp_file_disk_size_enough(8192);
  macro_id.set_third_id(73); // segment_id
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192/*valid_length*/);

  file_manager->set_tmp_file_cache_pause_gc();

  exhaust_tmp_file_disk_size(avail_size);
  write_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/, 8192 * 2/*valid_length*/, false/*is_sealed*/, write_buf_ + 8192);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192 * 2/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192 * 2/*valid_length*/);

  file_manager->set_tmp_file_cache_allow_gc();

  file_manager->calibrate_disk_space_task_.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, file_manager->calibrate_disk_space_task_.rm_logical_deleted_file());
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192 * 2/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192 * 2/*valid_length*/);

  // 18. concurrent append and calibration
  // (a) disk space enough, write local, unsealed, append local 3 times
  // (b) pause tmp file gc
  // (c) disk space not enough, write remote, unsealed (it will logical delete local file in io_callback)
  // (d) disk space enough, write local, unsealed
  // (e) allow tmp file gc, rm_logical_deleted_file
  release_tmp_file_disk_size(avail_size);
  check_tmp_file_disk_size_enough(8192);
  macro_id.set_third_id(74); // segment_id
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192/*valid_length*/);

  check_tmp_file_disk_size_enough(8192);
  write_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/, 8192 * 2/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 8192/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192 * 2/*valid_length*/);

  check_tmp_file_disk_size_enough(8192);
  write_tmp_file_data(macro_id, 8192 * 2/*offset*/, 8192/*size*/, 8192 * 3/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 8192 * 2/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192 * 3/*valid_length*/);

  file_manager->set_tmp_file_cache_pause_gc();

  exhaust_tmp_file_disk_size(avail_size);
  write_tmp_file_data(macro_id, 8192 * 3/*offset*/, 8192/*size*/, 8192 * 4/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 8192 * 3/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192 * 4/*valid_length*/);
  file_manager->set_tmp_file_cache_allow_gc();

  release_tmp_file_disk_size(avail_size);
  check_tmp_file_disk_size_enough(8192);
  write_tmp_file_data(macro_id, 8192 * 4/*offset*/, 8192/*size*/, 8192 * 5/*valid_length*/, false/*is_sealed*/, write_buf_);
  ASSERT_EQ(OB_SUCCESS, file_manager->calibrate_disk_space_task_.rm_logical_deleted_file());
  read_and_compare_tmp_file_data(macro_id, 8192 * 4/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192 * 5/*valid_length*/);
}

TEST_F(TestSSReaderWriter, private_tablet_meta_reader_writer)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  uint64_t ls_id = 1001;
  uint64_t ls_epoch_id = 1;
  uint64_t tablet_id = 200001;
  int64_t private_transfer_epoch = 0;
  int64_t version_id = 1;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_ls_id_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_meta_tablet_id_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_meta_tablet_id_private_transfer_epoch_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id, private_transfer_epoch));

  // 1. write to local cache
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_TABLET_META);
  macro_id.set_second_id(ls_id);
  macro_id.set_third_id(tablet_id);
  macro_id.set_meta_private_transfer_epoch(private_transfer_epoch);
  macro_id.set_meta_version_id(version_id); // meta_version_id
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.set_ls_epoch_id(ls_epoch_id);
  ObSSPrivateTabletMetaWriter private_tablet_meta_writer;
  ASSERT_EQ(OB_SUCCESS, private_tablet_meta_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  check_object_type_stat(macro_id, 0/*read_cnt*/, 0/*read_size*/, 1/*write_cnt*/, write_info_.size_, write_object_handle);
  write_object_handle.reset();

  // try overwrite PRIVATE_TABLET_META, expect return OB_FILE_ALREADY_EXIST errno
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  ASSERT_EQ(OB_SUCCESS, private_tablet_meta_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_FILE_ALREADY_EXIST, write_object_handle.wait());
  check_object_type_stat(macro_id, 0/*read_cnt*/, 0/*read_size*/, 1/*write_cnt*/, write_info_.size_, write_object_handle);
  write_object_handle.reset();

  // 2. read and compare the read data with the written data
  read_info_.macro_block_id_ = macro_id;
  read_info_.set_ls_epoch_id(ls_epoch_id);
  read_info_.offset_ = 0;
  read_info_.size_ = WRITE_IO_SIZE;
  ObStorageObjectHandle read_object_handle;
  ObSSPrivateTabletMetaReader private_tablet_meta_reader;
  ASSERT_EQ(OB_SUCCESS, private_tablet_meta_reader.aio_read(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  read_object_handle.reset();

  // 3. check object storage, expect not exist
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(macro_id, ls_epoch_id, is_exist));
  ASSERT_FALSE(is_exist);

  // 4. errsim OB_SERVER_OUTOF_DISK_SPACE
  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_DISK_OUTOF_SPACE_ERR, OB_SERVER_OUTOF_DISK_SPACE, 0, 1);

  // 5. write through to object storage
  macro_id.set_meta_version_id(2);
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  write_info_.set_ls_epoch_id(ls_epoch_id);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 6. read and compare the read data with the written data
  read_info_.macro_block_id_ = macro_id;
  read_info_.set_ls_epoch_id(ls_epoch_id);
  read_info_.offset_ = 0;
  read_info_.size_ = WRITE_IO_SIZE;
  ASSERT_EQ(OB_SUCCESS, private_tablet_meta_reader.aio_read(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  read_object_handle.reset();

  // 7. check object storage, expect exist
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(macro_id, ls_epoch_id, is_exist));
  ASSERT_TRUE(is_exist);

  TP_SET_EVENT(EventTable::EN_SHARED_STORAGE_DISK_OUTOF_SPACE_ERR, OB_SERVER_OUTOF_DISK_SPACE, 0, 0);
}

TEST_F(TestSSReaderWriter, private_macro_write_less_read_more)
{
  int ret = OB_SUCCESS;

  uint64_t tablet_id = 900;
  uint64_t server_id = 1;

  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_private_transfer_epoch_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*trasfer_seq*/));

  // 1. write 4KB
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(900); // seq_id
  macro_id.set_macro_private_transfer_epoch(0); // transfer_seq
  macro_id.set_tenant_seq(server_id);  //tenant_seq
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ObSSPrivateMacroWriter private_macro_writer;
  write_info_.offset_ = 0;
  write_info_.size_ = DIO_READ_ALIGN_SIZE; // 4KB
  ASSERT_EQ(OB_SUCCESS, private_macro_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  // 2. try read 2MB, expect real read 4KB
  read_info_.macro_block_id_ = macro_id;
  ObStorageObjectHandle read_object_handle;
  ObSSPrivateMacroReader private_macro_reader;
  read_info_.offset_ = 0;
  read_info_.size_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size(); // 2MB
  ASSERT_EQ(OB_SUCCESS, private_macro_reader.aio_read(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(DIO_READ_ALIGN_SIZE, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_, read_object_handle.get_buffer(), DIO_READ_ALIGN_SIZE));
}

TEST_F(TestSSReaderWriter, local_overwrite)
{
  int ret = OB_SUCCESS;

  uint64_t ls_id = 1;
  uint64_t ls_epoch_id = 1;
  uint64_t tablet_id = 800;
  int64_t transfer_seq = 0;

  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_ls_id_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_meta_tablet_id_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_meta_tablet_id_private_transfer_epoch_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id, transfer_seq));

  // 1. write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TENANT_DISK_SPACE_META);
  macro_id.set_second_id(MTL_ID());
  macro_id.set_third_id(MTL_EPOCH_ID());
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.set_ls_epoch_id(ls_epoch_id);

  ObSSLocalCacheWriter local_cache_writer;
  ASSERT_EQ(OB_SUCCESS, local_cache_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 2. read and compare the read data with the written data
  read_info_.macro_block_id_ = macro_id;
  read_info_.set_ls_epoch_id(ls_epoch_id);
  ObStorageObjectHandle read_object_handle;
  ObSSLocalCacheReader local_cache_reader;
  ASSERT_EQ(OB_SUCCESS, local_cache_reader.aio_read(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_, read_object_handle.get_buffer(), WRITE_IO_SIZE));
  read_object_handle.reset();

  // 3. overwrite
  char overwrite_buf[WRITE_IO_SIZE];
  for (int64_t i = 0; i < WRITE_IO_SIZE; ++i) {
    overwrite_buf[i] = write_buf_[i] + 1;
  }
  write_info_.buffer_ = overwrite_buf;
  write_object_handle.set_macro_block_id(macro_id);
  ASSERT_EQ(OB_SUCCESS, local_cache_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 4. read again, and compare the read data with the overwriten data
  ASSERT_EQ(OB_SUCCESS, local_cache_reader.aio_read(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(overwrite_buf, read_object_handle.get_buffer(), WRITE_IO_SIZE));
  ASSERT_NE(0, memcmp(write_buf_, read_object_handle.get_buffer(), WRITE_IO_SIZE));
  read_object_handle.reset();
}

TEST_F(TestSSReaderWriter, read_when_disable_micro_cache)
{
  int ret = OB_SUCCESS;

  uint64_t tablet_id = 200001;
  uint64_t data_seq = 15;

  // 1. write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(data_seq); // data_seq
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ObSSShareMacroWriter share_macro_writer;
  ASSERT_EQ(OB_SUCCESS, share_macro_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());


  // 2. disable micro cache, and then read <1, WRITE_IO_SIZE / 2>
  // expect bypass micro cache and read data from object storage successfully
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->disable_cache();

  ObLogicMicroBlockId logic_micro_id;
  logic_micro_id.version_ = ObLogicMicroBlockId::LOGIC_MICRO_ID_VERSION_V1;
  logic_micro_id.offset_ = 100;
  logic_micro_id.logic_macro_id_.data_seq_.macro_data_seq_ = 1;
  logic_micro_id.logic_macro_id_.logic_version_ = 100;
  logic_micro_id.logic_macro_id_.tablet_id_ = tablet_id;

  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  read_info_.set_logic_micro_id(logic_micro_id);
  read_info_.set_micro_crc(100);
  ObStorageObjectHandle object_handle;
  ObSSShareMacroReader share_macro_reader;
  ASSERT_EQ(OB_SUCCESS, share_macro_reader.aio_read(read_info_, object_handle));
  ASSERT_EQ(OB_SUCCESS, object_handle.wait());
  ASSERT_NE(nullptr, object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, object_handle.get_buffer(), read_info_.size_));
  object_handle.reset();
  micro_cache->enable_cache();
}

static void get_random_io_info(ObIOInfo &io_info)
{
  io_info.tenant_id_ = MTL_ID();
  io_info.fd_.first_id_ = ObIOFd::NORMAL_FILE_ID; // first_id is not used in shared storage mode
  io_info.fd_.second_id_ = MTL(ObTenantFileManager*)->get_micro_cache_file_fd();
  io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
  io_info.flag_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  io_info.timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
  io_info.offset_ = ObRandom::rand(1, 1000L * 1000L);
  io_info.size_ = ObRandom::rand(1, 1000L);
  io_info.flag_.set_read();
}

TEST_F(TestSSReaderWriter, test_batch_delete_tmp_files)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  const int64_t write_size = 8192; // 8KB

  // ======== Scenario 1: Test batch delete local tmp files ========
  LOG_INFO("=== Test batch delete local tmp files ===");
  check_tmp_file_disk_size_enough(write_size * 6);

  const int64_t local_file_count = 3;
  ObSEArray<MacroBlockId, local_file_count> local_file_ids;
  ObSEArray<int64_t, local_file_count> local_lengths;

  // Create and write local tmp files (100, 101, 102)
  for (int64_t i = 0; i < local_file_count; i++) {
    MacroBlockId file_id;
    int64_t tmp_file_id = 100 + i;
    file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    file_id.set_second_id(tmp_file_id);
    file_id.set_third_id(0);   // segment_id

    // Write first segment
    write_tmp_file_data(file_id, 0/*offset*/, write_size, write_size/*valid_length*/, false/*is_sealed*/, write_buf_);

    // Append more data for file 0 and 2
    if (i == 0 || i == 2) {
      write_tmp_file_data(file_id, write_size/*offset*/, write_size, write_size * 2/*valid_length*/, false/*is_sealed*/, write_buf_);
    }

    ASSERT_EQ(OB_SUCCESS, local_file_ids.push_back(file_id));
    int64_t length = (i == 0 || i == 2) ? 2 * write_size : write_size;
    ASSERT_EQ(OB_SUCCESS, local_lengths.push_back(length));
  }

  // Verify local files exist using segment metadata and read
  for (int64_t i = 0; i < local_file_count; i++) {
    check_tmp_file_seg_meta(local_file_ids.at(i), true/*is_meta_exist*/, true/*is_in_local*/, local_lengths.at(i)/*valid_length*/);
    read_and_compare_tmp_file_data(local_file_ids.at(i), 0/*offset*/, write_size/*size*/);
  }

  // Batch delete local tmp files
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_files(local_file_ids, local_lengths));

   // Verify local files are deleted - check segment metadata no longer exists
  for (int64_t i = 0; i < local_file_count; i++) {
    check_tmp_file_seg_meta(local_file_ids.at(i), false/*is_meta_exist*/);
  }
  LOG_INFO("=== Local tmp files batch delete test passed ===");

  // ======== Scenario 2: Test invalid arguments ========
  ObSEArray<MacroBlockId, 1> empty_file_ids;
  ObSEArray<int64_t, 1> empty_lengths;
  ASSERT_EQ(OB_INVALID_ARGUMENT, tenant_file_mgr->delete_tmp_files(empty_file_ids, empty_lengths));

  ObSEArray<MacroBlockId, 2> mismatch_file_ids;
  ObSEArray<int64_t, 1> mismatch_lengths;
  MacroBlockId dummy_file_id;
  dummy_file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  dummy_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  dummy_file_id.set_second_id(200);
  dummy_file_id.set_third_id(0);
  ASSERT_EQ(OB_SUCCESS, mismatch_file_ids.push_back(dummy_file_id));
  ASSERT_EQ(OB_SUCCESS, mismatch_file_ids.push_back(dummy_file_id));
  ASSERT_EQ(OB_SUCCESS, mismatch_lengths.push_back(write_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT, tenant_file_mgr->delete_tmp_files(mismatch_file_ids, mismatch_lengths));

  // ======== Scenario 3: Test batch delete remote tmp files ========
  LOG_INFO("=== Test batch delete remote tmp files ===");
  int64_t avail_size = 0;
  exhaust_tmp_file_disk_size(avail_size);

  const int64_t remote_file_count = 2;
  ObSEArray<MacroBlockId, remote_file_count> remote_file_ids;
  ObSEArray<int64_t, remote_file_count> remote_lengths;

  // Create remote tmp files (300, 301) with disk exhausted
  for (int64_t i = 0; i < remote_file_count; i++) {
    MacroBlockId remote_file_id;
    int64_t tmp_file_id = 300 + i;
    remote_file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    remote_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    remote_file_id.set_second_id(tmp_file_id);
    remote_file_id.set_third_id(0);   // segment_id

    // Write to remote (disk exhausted forces write through)
    write_tmp_file_data(remote_file_id, 0/*offset*/, write_size, write_size/*valid_length*/, false/*is_sealed*/, write_buf_);

    // Verify segment metadata exists (in remote)
    check_tmp_file_seg_meta(remote_file_id, true/*is_meta_exist*/, false/*is_in_local*/, write_size/*valid_length*/);

    ASSERT_EQ(OB_SUCCESS, remote_file_ids.push_back(remote_file_id));
    ASSERT_EQ(OB_SUCCESS, remote_lengths.push_back(write_size));
  }

  // Batch delete remote tmp files from object storage
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_files(remote_file_ids, remote_lengths));

  // Verify remote files are deleted - check segment metadata no longer exists
  for (int64_t i = 0; i < remote_file_count; i++) {
    check_tmp_file_seg_meta(remote_file_ids.at(i), false/*is_meta_exist*/);
  }

  // Release disk space for next test
  release_tmp_file_disk_size(avail_size);
  LOG_INFO("=== Remote tmp files batch delete test passed ===");
}

// Test concurrent conflict info creation to cover "fail to set refactored conflict info map" scenario
TEST_F(TestSSReaderWriter, test_concurrent_conflict_info_creation)
{
  static int64_t call_times = 0;
  call_times++;
  int ret = OB_SUCCESS;

  // Use a brand new tmp_file_id and segment_id that doesn't exist in the map
  const int64_t tmp_file_id = 9001;
  const int64_t segment_id = 8001;
  const TmpFileSegId seg_id(tmp_file_id, segment_id);

  ObTenantFileManager *file_mgr = MTL(ObTenantFileManager *);

  if (OB_NOT_NULL(file_mgr)) {
    ObSegmentFileManager *segment_file_mgr = &(file_mgr->get_segment_file_mgr());
    {
      LOG_INFO("Testing concurrent conflict info creation", K(seg_id), "call_times", call_times);

      // Create tmp file directory first
      ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id))
        << "call_times: " << call_times;

      // Create a valid macro block ID for tmp file
      MacroBlockId macro_id;
      macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
      macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
      macro_id.set_second_id(tmp_file_id);
      macro_id.set_third_id(segment_id);

      // Exhaust disk space to ensure file allocation works properly
      int64_t avail_size = 0;
      exhaust_tmp_file_disk_size(avail_size);

      // DO NOT pre-create conflict info - let threads race to create it
      // This will trigger the scenario where multiple threads try to insert the same seg_id

      // Use atomic counters to track results
      std::atomic<int> successful_creates(0);
      std::atomic<int> hash_exist_errors(0);
      std::atomic<int> other_errors(0);

      // Get MTL components in main thread to avoid multi-threading MTL issues
      uint64_t tenant_id = MTL_ID();

      // Create multiple threads to race for conflict info creation
      const int THREAD_COUNT = 5;
      std::thread threads[THREAD_COUNT];

      // Use a barrier to make all threads start at roughly the same time
      std::atomic<bool> start_flag(false);
      std::atomic<int> ready_count(0);

      for (int i = 0; i < THREAD_COUNT; i++) {
        threads[i] = std::thread([&, i, tenant_id, macro_id]() {
          // Signal ready and wait for start
          ready_count.fetch_add(1);
          while (!start_flag.load()) {
            std::this_thread::yield();
          }

          // Set MTL context for worker thread
          MTL_SWITCH(tenant_id) {
            ObStorageObjectWriteInfo write_info;
            ObStorageObjectHandle write_handle;

            write_info.size_ = 8192;
            write_info.buffer_ = write_buf_;
            write_info.offset_ = 0;
            write_info.io_desc_.set_wait_event(1);
            write_info.io_desc_.set_unsealed();
            write_info.io_timeout_ms_ = 10000;
            write_info.mtl_tenant_id_ = tenant_id;
            write_info.set_tmp_file_valid_length(8192);
            write_handle.macro_id_ = macro_id;

            // This will call get_or_create_conflict_info_handle internally
            // Multiple threads racing here will trigger the map insertion conflict
            int thread_ret = segment_file_mgr->async_append_file(write_info, write_handle);

            if (thread_ret == OB_SUCCESS) {
              successful_creates.fetch_add(1);
              LOG_INFO("Thread successfully created conflict info and started write", "thread_id", i, K(seg_id));
              usleep(50000); // 50ms to hold write lock briefly
            } else if (thread_ret == OB_ERR_EXCLUSIVE_LOCK_CONFLICT) {
              hash_exist_errors.fetch_add(1);
              LOG_INFO("Thread detected hash exist error", "thread_id", i, K(seg_id), KR(thread_ret));
            } else {
              other_errors.fetch_add(1);
              LOG_WARN("Thread failed with unexpected error", "thread_id", i, K(seg_id), KR(thread_ret));
            }
          }
        });
      }

      // Wait for all threads to be ready
      while (ready_count.load() < THREAD_COUNT) {
        usleep(1000);
      }

      // Release all threads at once to maximize race condition
      LOG_INFO("All threads ready, starting race", "ready_count", ready_count.load());
      start_flag.store(true);

      // Wait for all threads to complete
      for (int i = 0; i < THREAD_COUNT; i++) {
        threads[i].join();
      }

      LOG_INFO("Concurrent conflict info creation test completed",
               "successful_creates", successful_creates.load(),
               "hash_exist_errors", hash_exist_errors.load(),
               "other_errors", other_errors.load(),
               K(seg_id), "call_times", call_times);

      // Verify results:
      // - At least one thread should successfully create the conflict info
      // - Some threads should hit OB_HASH_EXIST (this is what we're testing for)
      // - Total attempts should equal thread count
      ASSERT_GE(successful_creates.load(), 1) << "At least one thread should succeed";

      if (hash_exist_errors.load() > 0) {
        LOG_INFO("SUCCESS: Covered 'fail to set refactored conflict info map' scenario",
                 "hash_exist_count", hash_exist_errors.load());
      } else {
        LOG_INFO("NOTE: Race condition not triggered this time (timing dependent)",
                 "successful_creates", successful_creates.load());
      }

      // Clean up: verify conflict info was created
      TmpFileConflictInfoHandle verify_handle;
      bool is_exist = false;
      ASSERT_EQ(OB_SUCCESS, segment_file_mgr->try_get_seg_conflict_info(seg_id, verify_handle, is_exist));
      ASSERT_TRUE(is_exist) << "Conflict info should exist after concurrent creation attempts";

      // Release exhausted disk space
      release_tmp_file_disk_size(avail_size);
    }
  }
}

// Test concurrent reads on the same file
TEST_F(TestSSReaderWriter, test_concurrent_reads_with_callbacks)
{
  static int64_t call_times = 0;
  call_times++;
  int ret = OB_SUCCESS;
  const int64_t tmp_file_id = 1003;
  const int64_t segment_id = 2003;
  const TmpFileSegId seg_id(tmp_file_id, segment_id);
  ObTenantFileManager *file_mgr = MTL(ObTenantFileManager *);

  if (OB_NOT_NULL(file_mgr)) {
    ObSegmentFileManager *segment_file_mgr = &(file_mgr->get_segment_file_mgr());
    {
      // Create tmp file directory first
      ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id)) << "call_times: " << call_times;

      // Create a valid macro block ID for tmp file
      MacroBlockId macro_id;
      macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
      macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
      macro_id.set_second_id(tmp_file_id);
      macro_id.set_third_id(segment_id);

      // First create actual file data using write_tmp_file_data
      int64_t avail_size = 0;
      exhaust_tmp_file_disk_size(avail_size);
      write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);

      // Verify that segment meta was created
      check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192/*valid_length*/);

      // Test concurrent reads on the same file
      LOG_INFO("Testing concurrent reads on the same file", K(seg_id));

      // Use atomic counters to track results
      std::atomic<int> successful_reads(0);
      std::atomic<int> failed_reads(0);

      // Get MTL components in main thread to avoid multi-threading MTL issues
      uint64_t tenant_id = MTL_ID();

      // Create multiple threads for concurrent reads
      const int READ_THREAD_COUNT = 3;
      std::thread read_threads[READ_THREAD_COUNT];

      for (int i = 0; i < READ_THREAD_COUNT; i++) {
        read_threads[i] = std::thread([&, i, tenant_id]() {
          // Set MTL context for worker thread
          MTL_SWITCH(tenant_id) {
            ObStorageObjectReadInfo read_info;
            ObStorageObjectHandle read_handle;
            char read_buf[8192];

            read_info.size_ = 8192;
            read_info.offset_ = 0;
            read_info.io_desc_.set_wait_event(1);
            read_info.io_desc_.set_unsealed();
            read_info.macro_block_id_ = macro_id;
            read_info.io_timeout_ms_ = 10000;
            read_info.mtl_tenant_id_ = tenant_id; // Use fixed tenant ID in multi-threaded context
            read_info.buf_ = read_buf;
            read_handle.macro_id_ = macro_id;

            int thread_ret = segment_file_mgr->async_pread_file(read_info, read_handle);

            if (thread_ret == OB_SUCCESS) {
              successful_reads.fetch_add(1);
              usleep(100000); // 100ms
              LOG_INFO("Concurrent read succeeded", "thread_id", i, K(seg_id));
            } else {
              failed_reads.fetch_add(1);
              LOG_WARN("Concurrent read failed", "thread_id", i, KR(thread_ret), K(seg_id));
            }
          }
        });
      }

      // Wait for all read threads to complete
      for (int i = 0; i < READ_THREAD_COUNT; i++) {
        read_threads[i].join();
      }

      LOG_INFO("Concurrent reads completed", "successful_reads", successful_reads.load(),
               "failed_reads", failed_reads.load(), K(seg_id));
      ASSERT_EQ(READ_THREAD_COUNT, successful_reads.load());
      ASSERT_EQ(0, failed_reads.load());

      // Release exhausted disk space
      release_tmp_file_disk_size(avail_size);
    }
  }
}

// Test concurrent writes on the same file (should fail with conflict detection)
TEST_F(TestSSReaderWriter, test_concurrent_writes_with_callbacks)
{
  static int64_t call_times = 0;
  call_times++;
  int ret = OB_SUCCESS;
  const int64_t tmp_file_id = 1004;
  const int64_t segment_id = 2004;
  const TmpFileSegId seg_id(tmp_file_id, segment_id);
  ObTenantFileManager *file_mgr = MTL(ObTenantFileManager *);

  if (OB_NOT_NULL(file_mgr)) {
    ObSegmentFileManager *segment_file_mgr = &(file_mgr->get_segment_file_mgr());
    {
      // Create tmp file directory first
      ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id)) << "call_times: " << call_times;

      // Create a valid macro block ID for tmp file
      MacroBlockId macro_id;
      macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
      macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
      macro_id.set_second_id(tmp_file_id);
      macro_id.set_third_id(segment_id);

      // First create actual file data using write_tmp_file_data
      int64_t avail_size = 0;
      exhaust_tmp_file_disk_size(avail_size);
      write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);

      // Verify that segment meta was created
      check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192/*valid_length*/);

      // Test concurrent writes on the same file (should detect conflict)
      LOG_INFO("Testing concurrent writes on the same file", K(seg_id));

      // Use atomic counters to track results
      std::atomic<int> successful_writes(0);
      std::atomic<int> failed_writes(0);
      std::atomic<int> conflict_detected(0);

      // Get MTL components in main thread to avoid multi-threading MTL issues
      uint64_t tenant_id = MTL_ID();

      // Create multiple threads for concurrent writes
      const int WRITE_THREAD_COUNT = 3;
      std::thread write_threads[WRITE_THREAD_COUNT];

      for (int i = 0; i < WRITE_THREAD_COUNT; i++) {
        write_threads[i] = std::thread([&, i, tenant_id]() {
          // Set MTL context for worker thread
          MTL_SWITCH(tenant_id) {
            ObStorageObjectWriteInfo write_info;
            ObStorageObjectHandle write_handle;

            write_info.size_ = 8192;
            write_info.buffer_ = write_buf_;
            write_info.offset_ = 0;
            write_info.io_desc_.set_wait_event(1);
            write_info.io_desc_.set_unsealed();
            write_info.io_timeout_ms_ = 10000;
            write_info.mtl_tenant_id_ = tenant_id; // Use fixed tenant ID in multi-threaded context
            write_info.set_tmp_file_valid_length(2 * 8192);
            write_handle.macro_id_ = macro_id;

            int thread_ret = segment_file_mgr->async_append_file(write_info, write_handle);

            if (thread_ret == OB_SUCCESS) {
              successful_writes.fetch_add(1);
              usleep(100000); // 100ms
              LOG_INFO("Concurrent write succeeded", "thread_id", i, K(seg_id));
            } else if (thread_ret == OB_ERR_UNEXPECTED) {
              conflict_detected.fetch_add(1);
              LOG_INFO("Write-write conflict detected as expected", "thread_id", i, K(seg_id));
            } else {
              failed_writes.fetch_add(1);
              LOG_WARN("Concurrent write failed with unexpected error", "thread_id", i, KR(thread_ret), K(seg_id));
            }
          }
        });
      }

      // Wait for all write threads to complete
      for (int i = 0; i < WRITE_THREAD_COUNT; i++) {
        write_threads[i].join();
      }

      LOG_INFO("Concurrent writes completed", "successful_writes", successful_writes.load(),
               "conflict_detected", conflict_detected.load(), "failed_writes", failed_writes.load(), K(seg_id));
      ASSERT_GE(successful_writes.load(), 1);
      ASSERT_EQ(failed_writes.load(), 0);

      // Release exhausted disk space
      release_tmp_file_disk_size(avail_size);
    }
  }
}

TEST_F(TestSSReaderWriter, test_concurrent_read_write_with_callbacks)
{
  static int64_t call_times = 0;
  call_times++;
  int ret = OB_SUCCESS;
  const int64_t tmp_file_id = 1001;
  const int64_t segment_id = 2001;
  const TmpFileSegId seg_id(tmp_file_id, segment_id);
  ObTenantFileManager *file_mgr = MTL(ObTenantFileManager *);

  if (OB_NOT_NULL(file_mgr)) {
    ObSegmentFileManager *segment_file_mgr = &(file_mgr->get_segment_file_mgr());
    {
      // Create tmp file directory first
      ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id)) << "call_times: " << call_times;

      // Create a valid macro block ID for tmp file
      MacroBlockId macro_id;
      macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
      macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
      macro_id.set_second_id(tmp_file_id);
      macro_id.set_third_id(segment_id);

      // First create actual file data using write_tmp_file_data
      int64_t avail_size = 0;
      exhaust_tmp_file_disk_size(avail_size);
      write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);

      // Verify that segment meta was created
      check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192/*valid_length*/);

      // Test concurrent read and write operations with callbacks
      LOG_INFO("Testing concurrent read-write operations with callbacks", K(seg_id));

      // Use atomic counters to track results
      std::atomic<int> successful_ops(0);
      std::atomic<int> failed_ops(0);
      std::atomic<int> conflict_detected(0);

      // Get MTL components in main thread to avoid multi-threading MTL issues
      uint64_t tenant_id = MTL_ID();

      // Create multiple threads for concurrent read and write operations
      const int TOTAL_THREAD_COUNT = 5;
      const int WRITE_THREAD_COUNT = 2;
      const int READ_THREAD_COUNT = 3;
      std::thread op_threads[TOTAL_THREAD_COUNT];

      // Create read threads
      for (int i = 0; i < READ_THREAD_COUNT; i++) {
        op_threads[WRITE_THREAD_COUNT + i] = std::thread([&, i, tenant_id]() {
          MTL_SWITCH(tenant_id) {
            ObStorageObjectReadInfo read_info;
            ObStorageObjectHandle read_handle;
            char read_buf[8192];

            read_info.size_ = 8192;
            read_info.offset_ = 0; // Different offsets for each thread
            read_info.io_desc_.set_wait_event(1);
            read_info.io_desc_.set_unsealed();
            read_info.macro_block_id_ = macro_id;
            read_info.io_timeout_ms_ = 10000;
            read_info.mtl_tenant_id_ = tenant_id; // Use fixed tenant ID in multi-threaded context
            read_info.buf_ = read_buf;
            read_handle.macro_id_ = macro_id;

            int thread_ret = segment_file_mgr->async_pread_file(read_info, read_handle);

            if (thread_ret == OB_SUCCESS) {
              successful_ops.fetch_add(1);
              usleep(100000); // 100ms
              LOG_INFO("Concurrent read succeeded", "read_thread_id", i, K(seg_id));
            } else if (thread_ret == OB_ERR_UNEXPECTED) {
              conflict_detected.fetch_add(1);
              LOG_INFO("Read-write conflict detected", "read_thread_id", i, K(seg_id));
            } else {
              failed_ops.fetch_add(1);
              LOG_WARN("Concurrent read failed", "read_thread_id", i, KR(thread_ret), K(seg_id));
            }
          }
        });
      }

      // Wait a bit to let read threads start before write threads
      usleep(10000); // 10ms

      // Create write threads
      for (int i = 0; i < WRITE_THREAD_COUNT; i++) {
        op_threads[i] = std::thread([&, i, tenant_id]() {
          MTL_SWITCH(tenant_id) {
            ObStorageObjectWriteInfo write_info;
            ObStorageObjectHandle write_handle;

            write_info.size_ = 8192;
            write_info.buffer_ = write_buf_;
            write_info.offset_ = 8192; // Different offsets for each thread
            write_info.io_desc_.set_wait_event(1);
            write_info.io_desc_.set_unsealed();
            write_info.io_timeout_ms_ = 10000;
            write_info.mtl_tenant_id_ = tenant_id; // Use tenant ID from main thread
            write_info.set_tmp_file_valid_length(2 * 8192);
            write_handle.macro_id_ = macro_id;

            int thread_ret = segment_file_mgr->async_append_file(write_info, write_handle);

            if (thread_ret == OB_SUCCESS) {
              successful_ops.fetch_add(1);
              LOG_INFO("Concurrent write succeeded", "write_thread_id", i, K(seg_id));
            } else if (thread_ret == OB_ERR_UNEXPECTED) {
              conflict_detected.fetch_add(1);
              LOG_INFO("Write conflict detected", "write_thread_id", i, K(seg_id));
            } else {
              failed_ops.fetch_add(1);
              LOG_WARN("Concurrent write failed", "write_thread_id", i, KR(thread_ret), K(seg_id));
            }
          }
        });
      }

      // Wait for all threads to complete
      for (int i = 0; i < TOTAL_THREAD_COUNT; i++) {
        op_threads[i].join();
      }

      LOG_INFO("Concurrent read-write operations completed", "successful_ops", successful_ops.load(),
               "conflict_detected", conflict_detected.load(), "failed_ops", failed_ops.load(), K(seg_id));
      ASSERT_GE(successful_ops.load(), READ_THREAD_COUNT);
      ASSERT_LE(conflict_detected.load(), WRITE_THREAD_COUNT);
      ASSERT_EQ(failed_ops.load(), 0);

      // Release exhausted disk space
      release_tmp_file_disk_size(avail_size);
    }
  }
}

TEST_F(TestSSReaderWriter, test_concurrent_write_read_with_callbacks)
{
  static int64_t call_times = 0;
  call_times++;
  int ret = OB_SUCCESS;
  const int64_t tmp_file_id = 1002;
  const int64_t segment_id = 2002;
  const TmpFileSegId seg_id(tmp_file_id, segment_id);
  ObTenantFileManager *file_mgr = MTL(ObTenantFileManager *);

  if (OB_NOT_NULL(file_mgr)) {
    ObSegmentFileManager *segment_file_mgr = &(file_mgr->get_segment_file_mgr());
    {
      // Create tmp file directory first
      ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id)) << "call_times: " << call_times;

      // Create a valid macro block ID for tmp file
      MacroBlockId macro_id;
      macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
      macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
      macro_id.set_second_id(tmp_file_id);
      macro_id.set_third_id(segment_id);

      // First create actual file data using write_tmp_file_data
      int64_t avail_size = 0;
      exhaust_tmp_file_disk_size(avail_size);
      write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);

      // Verify that segment meta was created
      check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, false/*is_in_local*/, 8192/*valid_length*/);

      // Test concurrent read and write operations with callbacks
      LOG_INFO("Testing concurrent write-read operations with callbacks", K(seg_id));

      // Use atomic counters to track results
      std::atomic<int> successful_ops(0);
      std::atomic<int> failed_ops(0);
      std::atomic<int> conflict_detected(0);

      // Get MTL components in main thread to avoid multi-threading MTL issues
      uint64_t tenant_id = MTL_ID();

      // Create multiple threads for concurrent read and write operations
      const int TOTAL_THREAD_COUNT = 5;
      const int WRITE_THREAD_COUNT = 2;
      const int READ_THREAD_COUNT = 3;
      std::thread op_threads[TOTAL_THREAD_COUNT];

      // Create write threads first
      for (int i = 0; i < WRITE_THREAD_COUNT; i++) {
        op_threads[i] = std::thread([&, i, tenant_id]() {
          // Set MTL context for worker thread
          MTL_SWITCH(tenant_id) {
            ObStorageObjectWriteInfo write_info;
            ObStorageObjectHandle write_handle;

            write_info.size_ = 8192;
            write_info.buffer_ = write_buf_;
            write_info.offset_ = 8192; // Same offset to test conflict detection
            write_info.io_desc_.set_wait_event(1);
            write_info.io_desc_.set_unsealed();
            write_info.io_timeout_ms_ = 10000;
            write_info.mtl_tenant_id_ = tenant_id; // Use tenant ID from main thread
            write_info.set_tmp_file_valid_length(2 * 8192);
            write_handle.macro_id_ = macro_id;

            int thread_ret = segment_file_mgr->async_append_file(write_info, write_handle);

            if (thread_ret == OB_SUCCESS) {
              successful_ops.fetch_add(1);
              LOG_INFO("Concurrent write succeeded", "write_thread_id", i, K(seg_id));
              // Keep write access active for a bit to ensure read conflicts
              usleep(100000); // 100ms delay to maintain write access
            } else if (thread_ret == OB_ERR_UNEXPECTED) {
              conflict_detected.fetch_add(1);
              LOG_INFO("Write conflict detected", "write_thread_id", i, K(seg_id));
            } else {
              failed_ops.fetch_add(1);
              LOG_WARN("Concurrent write failed", "write_thread_id", i, KR(thread_ret), K(seg_id));
            }
          }
        });
      }

      // Wait a bit to let first write thread start and acquire write access
      usleep(10000); // 10ms delay

      // Create read threads after write threads have started
      for (int i = 0; i < READ_THREAD_COUNT; i++) {
        op_threads[WRITE_THREAD_COUNT + i] = std::thread([&, i, tenant_id]() {
          // Set MTL context for worker thread
          MTL_SWITCH(tenant_id) {
            ObStorageObjectReadInfo read_info;
            ObStorageObjectHandle read_handle;
            char read_buf[8192];

            read_info.size_ = 8192;
            read_info.offset_ = 0;
            read_info.io_desc_.set_wait_event(1);
            read_info.io_desc_.set_unsealed();
            read_info.macro_block_id_ = macro_id;
            read_info.io_timeout_ms_ = 10000;
            read_info.mtl_tenant_id_ = tenant_id; // Use fixed tenant ID in multi-threaded context
            read_info.buf_ = read_buf;
            read_handle.macro_id_ = macro_id;

            int thread_ret = segment_file_mgr->async_pread_file(read_info, read_handle);

            if (thread_ret == OB_SUCCESS) {
              successful_ops.fetch_add(1);
              LOG_INFO("Concurrent read succeeded", "read_thread_id", i, K(seg_id));
            } else if (thread_ret == OB_ERR_UNEXPECTED) {
              conflict_detected.fetch_add(1);
              LOG_INFO("Read conflict detected", "read_thread_id", i, K(seg_id));
            } else {
              failed_ops.fetch_add(1);
              LOG_WARN("Concurrent read failed", "read_thread_id", i, KR(thread_ret), K(seg_id));
            }
          }
        });
      }

      // Wait for all threads to complete
      for (int i = 0; i < TOTAL_THREAD_COUNT; i++) {
        op_threads[i].join();
      }

      LOG_INFO("Concurrent write-read operations completed", "successful_ops", successful_ops.load(),
               "conflict_detected", conflict_detected.load(), "failed_ops", failed_ops.load(), K(seg_id));
      ASSERT_GE(successful_ops.load(), 1);
      ASSERT_EQ(failed_ops.load(), 0);

      // Release exhausted disk space
      release_tmp_file_disk_size(avail_size);
    }
  }
}

// Test concurrent access control fixes
TEST_F(TestSSReaderWriter, test_read_callback_destructor_cleanup)
{
  int ret = OB_SUCCESS;

  // Create conflict info
  TmpFileConflictInfoHandle conflict_info_handle;
  ASSERT_EQ(OB_SUCCESS, conflict_info_handle.set_tmpfile_conflict_info(false, 0));

  // Acquire read access
  ASSERT_EQ(OB_SUCCESS, conflict_info_handle.try_acquire_read_access());
  ASSERT_EQ(1, conflict_info_handle.get_conflict_info()->active_readers_count_);

  // Create read callback (simulating the scenario where callback_ is null)
  ObSSTmpFileReadCallback *read_callback = nullptr;
  ObMalloc allocator;

  read_callback = static_cast<ObSSTmpFileReadCallback *>(
      allocator.alloc(sizeof(ObSSTmpFileReadCallback)));
  ASSERT_NE(nullptr, read_callback);

  // Initialize with null callback (this is the problematic scenario)
  read_callback = new (read_callback) ObSSTmpFileReadCallback(&allocator, nullptr, nullptr);

  TmpFileSegId seg_id(60009, 0);
  // Create a mock segment file manager for testing
  ObSegmentFileManager mock_seg_mgr;
  ASSERT_EQ(OB_SUCCESS, read_callback->set_ss_tmpfile_read_callback(
      seg_id, &mock_seg_mgr, &allocator, conflict_info_handle));

  // Verify read access is still held
  ASSERT_EQ(1, conflict_info_handle.get_conflict_info()->active_readers_count_);

  // Destroy callback - this should release read access even with null callback_
  read_callback->~ObSSTmpFileReadCallback();
  allocator.free(read_callback);

  // Verify read access was properly released
  ASSERT_EQ(0, conflict_info_handle.get_conflict_info()->active_readers_count_);

  LOG_INFO("test_read_callback_destructor_cleanup passed - read access properly released");
}

// Test concurrent read-write operations
TEST_F(TestSSReaderWriter, test_concurrent_read_write_operations)
{
  const int THREAD_COUNT = 5;
  const int OPERATIONS_PER_THREAD = 10;

  std::atomic<int> successful_reads(0);
  std::atomic<int> successful_writes(0);
  std::atomic<int> read_conflicts(0);
  std::atomic<int> write_conflicts(0);
  std::atomic<int> other_errors(0);

  // Create shared conflict info
  TmpFileConflictInfoHandle shared_conflict_info;
  ASSERT_EQ(OB_SUCCESS, shared_conflict_info.set_tmpfile_conflict_info(false, 0));

  std::vector<std::thread> threads;

  // Create mixed read/write threads
  for (int i = 0; i < THREAD_COUNT; i++) {
    threads.emplace_back([&, i]() {
      for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
        bool is_write_op = (i % 2 == 0); // Alternate between read and write threads

        if (is_write_op) {
          // Try write operation
          TmpFileConflictInfoHandle local_handle;
          if (OB_SUCCESS == local_handle.assign(shared_conflict_info)) {
            int ret = local_handle.try_acquire_write_access();
            if (OB_SUCCESS == ret) {
              successful_writes.fetch_add(1);
              // Simulate some work
              std::this_thread::sleep_for(std::chrono::microseconds(100));
              local_handle.release_write_access();
            } else if (OB_ERR_UNEXPECTED == ret) {
              write_conflicts.fetch_add(1);
            } else {
              other_errors.fetch_add(1);
            }
          }
        } else {
          // Try read operation
          TmpFileConflictInfoHandle local_handle;
          if (OB_SUCCESS == local_handle.assign(shared_conflict_info)) {
            int ret = local_handle.try_acquire_read_access();
            if (OB_SUCCESS == ret) {
              successful_reads.fetch_add(1);
              // Simulate some work
              std::this_thread::sleep_for(std::chrono::microseconds(50));
              local_handle.release_read_access();
            } else if (OB_ERR_UNEXPECTED == ret) {
              read_conflicts.fetch_add(1);
            } else {
              other_errors.fetch_add(1);
            }
          }
        }

        // Small delay between operations
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  // Verify final state - this is the critical test for our fix
  ASSERT_EQ(0, shared_conflict_info.get_conflict_info()->active_readers_count_);
  ASSERT_FALSE(shared_conflict_info.get_conflict_info()->is_writing_);

  // Log statistics
  LOG_INFO("Concurrent operations completed",
           "successful_reads", successful_reads.load(),
           "successful_writes", successful_writes.load(),
           "read_conflicts", read_conflicts.load(),
           "write_conflicts", write_conflicts.load(),
           "other_errors", other_errors.load());

  // Verify we had some successful operations
  ASSERT_GT(successful_reads.load() + successful_writes.load(), 0);

  LOG_INFO("test_concurrent_read_write_operations passed - no resource leaks detected");
}

// Test that write access is properly released on callback destruction
TEST_F(TestSSReaderWriter, test_write_callback_destructor_cleanup)
{
  int ret = OB_SUCCESS;

  // Create conflict info
  TmpFileConflictInfoHandle conflict_info_handle;
  ASSERT_EQ(OB_SUCCESS, conflict_info_handle.set_tmpfile_conflict_info(false, 0));

  // Acquire write access
  ASSERT_EQ(OB_SUCCESS, conflict_info_handle.try_acquire_write_access());
  ASSERT_TRUE(conflict_info_handle.get_conflict_info()->is_writing_);

  // Create write callback
  ObSSTmpFileWriteCallback *write_callback = nullptr;
  ObMalloc allocator;

  write_callback = static_cast<ObSSTmpFileWriteCallback *>(
      allocator.alloc(sizeof(ObSSTmpFileWriteCallback)));
  ASSERT_NE(nullptr, write_callback);

  write_callback = new (write_callback) ObSSTmpFileWriteCallback();

  TmpFileSegId seg_id(60009, 0);
  TmpFileMetaHandle meta_handle;
  // Create a valid meta handle for testing
  ASSERT_EQ(OB_SUCCESS, meta_handle.set_tmpfile_meta(true, 1024, 1024));
  ASSERT_EQ(OB_SUCCESS, write_callback->set_ss_tmpfile_write_callback(
      seg_id, meta_handle, conflict_info_handle,
      ObSSTmpFileSegMetaOpType::INSERT, ObSSTmpFileSegDeleteType::NONE,
      false, 0, &allocator));

  // Verify write access is still held
  ASSERT_TRUE(conflict_info_handle.get_conflict_info()->is_writing_);

  // Destroy callback - this should release write access
  write_callback->~ObSSTmpFileWriteCallback();
  allocator.free(write_callback);

  // Verify write access was properly released
  ASSERT_FALSE(conflict_info_handle.get_conflict_info()->is_writing_);

  LOG_INFO("test_write_callback_destructor_cleanup passed - write access properly released");
}

// Test dual write mode for 2MB sealed tmp file
TEST_F(TestSSReaderWriter, tmp_file_2MB_sealed_dual_write)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  // to avoid affecting tmp file seg_meta_map and tmp file tmp_file_write_free_disk_size
  // disable tmp_file_flush_task, preread_task_, calibrate_disk_space_task and gc_unsealed_tmp_file_task
  // preread_task_ will affect local disk size, so preread_task_ need to disble
  file_manager->preread_cache_mgr_.preread_task_.is_inited_ = false;
  macro_cache_mgr->evict_task_.is_inited_ = false;
  macro_cache_mgr->flush_task_.is_inited_ = false;
  file_manager->calibrate_disk_space_task_.is_inited_ = false;
  file_manager->segment_file_mgr_.gc_segment_file_task_.is_inited_ = false;
  sleep(3);

  uint64_t tmp_file_id = 300;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));

  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id);

  LOG_INFO("=== Test 1: Dual write mode with local space available ===");

  // Ensure sufficient disk space
  check_tmp_file_disk_size_enough(OB_DEFAULT_MACRO_BLOCK_SIZE);

  // Write 2MB sealed segment
  macro_id.set_third_id(200);
  write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/,
                      OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);

  // Verify data can be read correctly from local cache
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);

  // For dual write with local space available: local write succeeds, meta needed
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/);

  // Check if file exists in local cache
  bool is_local_exist = false;
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_local_exist));
  ASSERT_TRUE(is_local_exist);
  LOG_INFO("dual write test 1 passed: file exists in local cache", K(macro_id), K(is_local_exist));

  LOG_INFO("=== Test 2: Overwrite 8KB unsealed with 2MB sealed (dual write) ===");

  // First write 8KB unsealed segment
  macro_id.set_third_id(201);
  check_tmp_file_disk_size_enough(8192);
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/,
                      false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);

  // Verify 8KB segment has meta
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192/*valid_length*/);

  // Then write 2MB sealed segment (should trigger dual write mode)
  check_tmp_file_disk_size_enough(OB_DEFAULT_MACRO_BLOCK_SIZE);
  write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/,
                      OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);

  // Verify 2MB sealed data can be read correctly
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);

  // After 2MB sealed write, meta should exist because write local need meta
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/);

  LOG_INFO("dual write test 2 passed: 8KB to 2MB overwrite works correctly");

  LOG_INFO("=== Test 3: Write_through fallback when disk space insufficient ===");

  // Exhaust disk space
  int64_t avail_size = 0;
  exhaust_tmp_file_disk_size(avail_size);

  // Write 2MB sealed segment (should fallback to write_through)
  macro_id.set_third_id(202);
  write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/,
                      OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);

  // Verify data can be read from object storage
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);

  // write_through mode, no meta for sealed segment
  check_tmp_file_seg_meta(macro_id, false/*is_meta_exist*/);

  LOG_INFO("dual write test 3 passed: write_through fallback works when disk full");
  // Cleanup
  release_tmp_file_disk_size(avail_size);

  LOG_INFO("=== All dual write tests passed! ===");
}

// Test async_write_dual file size allocation correctness
TEST_F(TestSSReaderWriter, test_async_write_dual_alloc_file_size)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);

  // Disable background tasks to avoid interference
  file_manager->preread_cache_mgr_.preread_task_.is_inited_ = false;
  macro_cache_mgr->evict_task_.is_inited_ = false;
  macro_cache_mgr->flush_task_.is_inited_ = false;
  file_manager->calibrate_disk_space_task_.is_inited_ = false;
  file_manager->segment_file_mgr_.gc_segment_file_task_.is_inited_ = false;
  sleep(3);

  uint64_t tmp_file_id = 400;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));

  MacroBlockId macro_id;
  macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  macro_id.set_second_id(tmp_file_id);

  // ========================================================================
  // Test 1: Verify file size allocation when local space is available
  // ========================================================================
  LOG_INFO("=== Test 1: Verify file size allocation with sufficient local space ===");

  check_tmp_file_disk_size_enough(OB_DEFAULT_MACRO_BLOCK_SIZE);

  // Record initial used size
  int64_t initial_used_size = disk_space_mgr->get_macro_cache_used_size();
  LOG_INFO("Initial disk usage", K(initial_used_size));

  // Write 2MB sealed segment using async_write_dual
  macro_id.set_third_id(300);
  write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/,
                      OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);

  // Verify file size was allocated (used size should increase by 2MB)
  int64_t after_write_used_size = disk_space_mgr->get_macro_cache_used_size();
  int64_t allocated_size = after_write_used_size - initial_used_size;
  LOG_INFO("After write disk usage", K(after_write_used_size), K(allocated_size));

  // The allocated size should be approximately 2MB (may have alignment overhead)
  ASSERT_GE(allocated_size, OB_DEFAULT_MACRO_BLOCK_SIZE)
      << "File size should be allocated. initial=" << initial_used_size
      << ", after=" << after_write_used_size
      << ", diff=" << allocated_size;

  // Verify data can be read correctly
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);

  // Verify meta exists (local write with meta)
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/,
                          OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/);

  LOG_INFO("Test 1 passed: File size allocated correctly for local write");

  // ========================================================================
  // Test 2: Verify fallback when disk space is insufficient (no alloc needed)
  // ========================================================================
  LOG_INFO("=== Test 2: Verify fallback when disk space is insufficient ===");

  // Exhaust disk space
  int64_t avail_size = 0;
  exhaust_tmp_file_disk_size(avail_size);

  int64_t before_fallback_used_size = disk_space_mgr->get_macro_cache_used_size();
  LOG_INFO("Before fallback disk usage", K(before_fallback_used_size));

  // Write 2MB sealed segment (should fallback to write_through without alloc)
  macro_id.set_third_id(301);
  write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/,
                      OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);

  // Verify no additional file size was allocated (write_through mode)
  int64_t after_fallback_used_size = disk_space_mgr->get_macro_cache_used_size();
  int64_t fallback_allocated = after_fallback_used_size - before_fallback_used_size;
  LOG_INFO("After fallback disk usage", K(after_fallback_used_size), K(fallback_allocated));

  ASSERT_EQ(0, fallback_allocated)
      << "No file size should be allocated in write_through mode. before="
      << before_fallback_used_size << ", after=" << after_fallback_used_size;

  // Verify data can still be read from object storage
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);

  // Verify no meta exists (write_through mode for sealed segment)
  check_tmp_file_seg_meta(macro_id, false/*is_meta_exist*/);

  LOG_INFO("Test 2 passed: Fallback works correctly without allocating file size");

  // Restore disk space
  release_tmp_file_disk_size(avail_size);

  // ========================================================================
  // Test 3: Verify file size allocation for multiple segments
  // ========================================================================
  LOG_INFO("=== Test 3: Verify file size allocation for multiple segments ===");

  check_tmp_file_disk_size_enough(OB_DEFAULT_MACRO_BLOCK_SIZE * 3);

  int64_t before_multi_write = disk_space_mgr->get_macro_cache_used_size();
  LOG_INFO("Before multiple writes", K(before_multi_write));

  // Write 3 sealed segments
  for (int i = 0; i < 3; i++) {
    macro_id.set_third_id(302 + i);
    write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/,
                        OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);
  }

  int64_t after_multi_write = disk_space_mgr->get_macro_cache_used_size();
  int64_t multi_allocated = after_multi_write - before_multi_write;
  LOG_INFO("After multiple writes", K(after_multi_write), K(multi_allocated));

  // Verify approximately 6MB was allocated (3 segments * 2MB each)
  ASSERT_GE(multi_allocated, OB_DEFAULT_MACRO_BLOCK_SIZE * 3)
      << "File size for 3 segments should be allocated. before=" << before_multi_write
      << ", after=" << after_multi_write << ", diff=" << multi_allocated;

  // Verify all 3 segments can be read
  for (int i = 0; i < 3; i++) {
    macro_id.set_third_id(302 + i);
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);
    check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/,
                            OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/);
  }

  LOG_INFO("Test 3 passed: Multiple segments allocated file size correctly");

  // ========================================================================
  // Test 4: Verify overwrite scenario (8KB unsealed -> 2MB sealed)
  // ========================================================================
  LOG_INFO("=== Test 4: Verify file size allocation for overwrite scenario ===");

  check_tmp_file_disk_size_enough(OB_DEFAULT_MACRO_BLOCK_SIZE);

  // First write 8KB unsealed segment
  macro_id.set_third_id(305);
  int64_t before_small_write = disk_space_mgr->get_macro_cache_used_size();
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/,
                      false/*is_sealed*/, write_buf_);
  int64_t after_small_write = disk_space_mgr->get_macro_cache_used_size();
  int64_t small_allocated = after_small_write - before_small_write;
  LOG_INFO("Small segment allocated", K(small_allocated));

  ASSERT_GE(small_allocated, 8192) << "8KB should be allocated";

  // Then overwrite with 2MB sealed segment using async_write_dual
  check_tmp_file_disk_size_enough(OB_DEFAULT_MACRO_BLOCK_SIZE);
  int64_t before_overwrite = disk_space_mgr->get_macro_cache_used_size();
  write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/,
                      OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);
  int64_t after_overwrite = disk_space_mgr->get_macro_cache_used_size();
  int64_t overwrite_allocated = after_overwrite - before_overwrite;
  LOG_INFO("Overwrite allocated", K(before_overwrite), K(after_overwrite), K(overwrite_allocated));

  // Additional ~2MB should be allocated for the 2MB segment
  ASSERT_GE(overwrite_allocated, OB_DEFAULT_MACRO_BLOCK_SIZE - 8192)
      << "Additional file size should be allocated for overwrite. before=" << before_overwrite
      << ", after=" << after_overwrite << ", diff=" << overwrite_allocated;

  // Verify 2MB data can be read
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/,
                          OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/);

  LOG_INFO("Test 4 passed: Overwrite scenario allocates file size correctly");

  LOG_INFO("=== All async_write_dual file size allocation tests passed! ===");
}

// Test disk space threshold: local write -> fallback to object storage -> recover to local write
TEST_F(TestSSReaderWriter, test_disk_space_threshold_fallback_and_recovery)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);

  // Disable background tasks
  file_manager->preread_cache_mgr_.preread_task_.is_inited_ = false;
  macro_cache_mgr->evict_task_.is_inited_ = false;
  macro_cache_mgr->flush_task_.is_inited_ = false;
  file_manager->calibrate_disk_space_task_.is_inited_ = false;
  file_manager->segment_file_mgr_.gc_segment_file_task_.is_inited_ = false;
  sleep(3);

  uint64_t tmp_file_id = 500;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));

  MacroBlockId macro_id;
  macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  macro_id.set_second_id(tmp_file_id);

  // ========================================================================
  // Phase 1: Write to local disk when space is available
  // ========================================================================
  LOG_INFO("=== Phase 1: Write 2MB sealed segments to local disk ===");

  check_tmp_file_disk_size_enough(OB_DEFAULT_MACRO_BLOCK_SIZE * 10);

  int64_t initial_used = disk_space_mgr->get_macro_cache_used_size();
  LOG_INFO("Initial disk usage", K(initial_used));

  // Write multiple 2MB sealed segments to local disk
  const int LOCAL_WRITE_COUNT = 5;
  for (int i = 0; i < LOCAL_WRITE_COUNT; i++) {
    macro_id.set_third_id(400 + i);
    write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/,
                        OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);

    // Verify written to local with meta
    check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/,
                            OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/);
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);

    LOG_INFO("Written segment to local", K(i), "segment_id", macro_id.third_id());
  }

  int64_t after_local_writes = disk_space_mgr->get_macro_cache_used_size();
  int64_t local_allocated = after_local_writes - initial_used;
  LOG_INFO("After local writes", K(after_local_writes), K(local_allocated),
           "segments", LOCAL_WRITE_COUNT);

  ASSERT_EQ(local_allocated, OB_DEFAULT_MACRO_BLOCK_SIZE * LOCAL_WRITE_COUNT)
      << "Local disk space should be allocated for " << LOCAL_WRITE_COUNT << " segments";

  LOG_INFO("Phase 1 passed: All segments written to local disk");

  // ========================================================================
  // Phase 2: Exhaust disk space and fallback to object storage
  // ========================================================================
  LOG_INFO("=== Phase 2: Exhaust disk space and fallback to object storage ===");

  // Exhaust disk space to trigger fallback
  int64_t exhausted_size = 0;
  exhaust_tmp_file_disk_size(exhausted_size);

  int64_t before_fallback = disk_space_mgr->get_macro_cache_used_size();
  LOG_INFO("Disk space exhausted", K(before_fallback), K(exhausted_size));

  // Write multiple 2MB sealed segments (should fallback to object storage)
  const int FALLBACK_WRITE_COUNT = 3;
  for (int i = 0; i < FALLBACK_WRITE_COUNT; i++) {
    macro_id.set_third_id(500 + i);
    write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/,
                        OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);

    // Verify written to object storage (no meta for sealed segments in write_through mode)
    check_tmp_file_seg_meta(macro_id, false/*is_meta_exist*/);
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);

    LOG_INFO("Written segment to object storage (fallback)", K(i), "segment_id", macro_id.third_id());
  }

  int64_t after_fallback = disk_space_mgr->get_macro_cache_used_size();
  int64_t fallback_allocated = after_fallback - before_fallback;
  LOG_INFO("After fallback writes", K(after_fallback), K(fallback_allocated),
           "segments", FALLBACK_WRITE_COUNT);

  ASSERT_EQ(0, fallback_allocated)
      << "No local disk space should be allocated in fallback mode";

  LOG_INFO("Phase 2 passed: Fallback to object storage when disk is full");

  // ========================================================================
  // Phase 3: Release disk space and verify recovery to local writes
  // ========================================================================
  LOG_INFO("=== Phase 3: Release disk space and recover to local writes ===");

  // Release the exhausted disk space
  release_tmp_file_disk_size(exhausted_size);

  // Ensure sufficient space is available
  check_tmp_file_disk_size_enough(OB_DEFAULT_MACRO_BLOCK_SIZE * 5);

  int64_t before_recovery = disk_space_mgr->get_macro_cache_used_size();
  LOG_INFO("Disk space released", K(before_recovery));

  // Write multiple 2MB sealed segments (should write to local again)
  const int RECOVERY_WRITE_COUNT = 3;
  for (int i = 0; i < RECOVERY_WRITE_COUNT; i++) {
    macro_id.set_third_id(600 + i);
    write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/,
                        OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);

    // Verify written to local with meta (recovered to local write mode)
    check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/,
                            OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/);
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);

    LOG_INFO("Written segment to local (recovered)", K(i), "segment_id", macro_id.third_id());
  }

  int64_t after_recovery = disk_space_mgr->get_macro_cache_used_size();
  int64_t recovery_allocated = after_recovery - before_recovery;
  LOG_INFO("After recovery writes", K(after_recovery), K(recovery_allocated),
           "segments", RECOVERY_WRITE_COUNT);

  ASSERT_EQ(recovery_allocated, OB_DEFAULT_MACRO_BLOCK_SIZE * RECOVERY_WRITE_COUNT)
      << "Local disk space should be allocated again after recovery";

  LOG_INFO("Phase 3 passed: Recovered to local writes after releasing disk space");

  // ========================================================================
  // Phase 4: Verify all data is readable
  // ========================================================================
  LOG_INFO("=== Phase 4: Verify all written data is readable ===");

  // Verify Phase 1 local writes
  for (int i = 0; i < LOCAL_WRITE_COUNT; i++) {
    macro_id.set_third_id(400 + i);
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);
  }

  // Verify Phase 2 fallback writes
  for (int i = 0; i < FALLBACK_WRITE_COUNT; i++) {
    macro_id.set_third_id(500 + i);
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);
  }

  // Verify Phase 3 recovery writes
  for (int i = 0; i < RECOVERY_WRITE_COUNT; i++) {
    macro_id.set_third_id(600 + i);
    read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);
  }

  LOG_INFO("Phase 4 passed: All data readable across all phases");

  LOG_INFO("=== All disk space threshold fallback and recovery tests passed! ===",
           "local_writes", LOCAL_WRITE_COUNT,
           "fallback_writes", FALLBACK_WRITE_COUNT,
           "recovery_writes", RECOVERY_WRITE_COUNT);
}

TEST_F(TestSSReaderWriter, performance_comparison_write_through_vs_write_dual)
{
  // Performance comparison test:
  // Compare write_through (old: all to object storage) vs write_dual (new: prefer local cache)
  // Write 100 x 2MB sealed segments and measure time difference
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  // to avoid affecting tmp file seg_meta_map and tmp file tmp_file_write_free_disk_size
  // disable tmp_file_flush_task, preread_task_, calibrate_disk_space_task and gc_unsealed_tmp_file_task
  // preread_task_ will affect local disk size, so preread_task_ need to disble
  file_manager->preread_cache_mgr_.preread_task_.is_inited_ = false;
  macro_cache_mgr->evict_task_.is_inited_ = false;
  macro_cache_mgr->flush_task_.is_inited_ = false;
  file_manager->calibrate_disk_space_task_.is_inited_ = false;
  file_manager->segment_file_mgr_.gc_segment_file_task_.is_inited_ = false;
  sleep(3);

  const int64_t segment_count = 1000;
  const int64_t segment_size = OB_DEFAULT_MACRO_BLOCK_SIZE; // 2MB
  LOG_INFO("=== Performance Comparison Test Start ===", K(segment_count), K(segment_size));

  // ===========================================
  // Scenario 1: write_through mode (old behavior - all to object storage)
  // ===========================================
  LOG_INFO("--- Scenario 1: write_through mode (force write to object storage) ---");

  // Exhaust local disk space to force write_through
  int64_t avail_size = 0;
  exhaust_tmp_file_disk_size(avail_size);
  ASSERT_GT(avail_size, 0);

  int64_t start_time_write_through = ObTimeUtility::current_time();

  for (int64_t i = 0; i < segment_count; ++i) {
    MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(1000);
    macro_id.set_third_id(1000 + i); // unique segment id for each iteration
    macro_id.set_macro_private_transfer_epoch(0);
    macro_id.set_tenant_seq(0);

    // Write sealed segment (will use write_through because disk is exhausted)
    write_tmp_file_data(macro_id, 0/*offset*/, segment_size, segment_size/*valid_length*/, true/*is_sealed*/, write_buf_);

    if ((i + 1) % 20 == 0) {
      LOG_INFO("write_through progress", K(i), K(segment_count));
    }
  }

  int64_t end_time_write_through = ObTimeUtility::current_time();
  int64_t duration_write_through_us = end_time_write_through - start_time_write_through;
  int64_t duration_write_through_ms = duration_write_through_us / 1000;

  LOG_INFO("Scenario 1 completed", "duration_ms", duration_write_through_ms,
           "duration_us", duration_write_through_us,
           "avg_per_segment_ms", duration_write_through_ms / segment_count);

  // Restore local disk space
  release_tmp_file_disk_size(avail_size);


  // ===========================================
  // Scenario 2a: write_dual mode with sufficient space (all to local cache)
  // ===========================================
  LOG_INFO("--- Scenario 2a: write_dual mode (sufficient space - all to local) ---");

  int64_t start_time_write_dual_full = ObTimeUtility::current_time();

  for (int64_t i = 0; i < segment_count; ++i) {
    MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(1001);
    macro_id.set_third_id(2000 + i); // unique segment id for each iteration
    macro_id.set_macro_private_transfer_epoch(0);
    macro_id.set_tenant_seq(0);

    // Write sealed segment (will use write_dual and write to local cache)
    write_tmp_file_data(macro_id, 0/*offset*/, segment_size, segment_size/*valid_length*/, true/*is_sealed*/, write_buf_);

    if ((i + 1) % 20 == 0) {
      LOG_INFO("write_dual_full progress", K(i), K(segment_count));
    }
  }

  int64_t end_time_write_dual_full = ObTimeUtility::current_time();
  int64_t duration_write_dual_full_us = end_time_write_dual_full - start_time_write_dual_full;
  int64_t duration_write_dual_full_ms = duration_write_dual_full_us / 1000;

  LOG_INFO("Scenario 2a completed", "duration_ms", duration_write_dual_full_ms,
           "duration_us", duration_write_dual_full_us,
           "avg_per_segment_ms", duration_write_dual_full_ms / segment_count);

  // ===========================================
  // Scenario 2b: write_dual mode with 75% local (25% remote)
  // ===========================================
  LOG_INFO("--- Scenario 2b: write_dual mode (75% local / 25% remote) ---");

  const int64_t three_quarter_count = (segment_count * 3) / 4;
  int64_t start_time_write_dual_three_quarter = ObTimeUtility::current_time();

  // Phase 1: Write first 75% to local
  LOG_INFO("Phase 1: Writing first 75% segments to local cache...");
  for (int64_t i = 0; i < three_quarter_count; ++i) {
    MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(1002);
    macro_id.set_third_id(3000 + i);
    macro_id.set_macro_private_transfer_epoch(0);
    macro_id.set_tenant_seq(0);

    write_tmp_file_data(macro_id, 0/*offset*/, segment_size, segment_size/*valid_length*/, true/*is_sealed*/, write_buf_);

    if ((i + 1) % 20 == 0) {
      LOG_INFO("write_dual_three_quarter phase1 progress", K(i), K(three_quarter_count));
    }
  }
  LOG_INFO("Phase 1 completed: first 75% written to local");

  // Phase 2: Exhaust space, then write remaining 25% to remote
  LOG_INFO("Phase 2: Exhausting space, then writing remaining 25% to remote...");
  int64_t exhausted_size_three_quarter = 0;
  exhaust_tmp_file_disk_size(exhausted_size_three_quarter);
  LOG_INFO("Exhausted disk space for phase 2", K(exhausted_size_three_quarter));

  for (int64_t i = three_quarter_count; i < segment_count; ++i) {
    MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(1002);
    macro_id.set_third_id(3000 + i);
    macro_id.set_macro_private_transfer_epoch(0);
    macro_id.set_tenant_seq(0);

    write_tmp_file_data(macro_id, 0/*offset*/, segment_size, segment_size/*valid_length*/, true/*is_sealed*/, write_buf_);

    if ((i + 1) % 20 == 0) {
      LOG_INFO("write_dual_three_quarter phase2 progress", K(i), K(segment_count));
    }
  }
  LOG_INFO("Phase 2 completed: remaining 25% written to remote");

  int64_t end_time_write_dual_three_quarter = ObTimeUtility::current_time();
  int64_t duration_write_dual_three_quarter_us = end_time_write_dual_three_quarter - start_time_write_dual_three_quarter;
  int64_t duration_write_dual_three_quarter_ms = duration_write_dual_three_quarter_us / 1000;

  LOG_INFO("Scenario 2b completed", "duration_ms", duration_write_dual_three_quarter_ms,
           "duration_us", duration_write_dual_three_quarter_us,
           "avg_per_segment_ms", duration_write_dual_three_quarter_ms / segment_count);

  release_tmp_file_disk_size(exhausted_size_three_quarter);

  // ===========================================
  // Scenario 2c: write_dual mode with 50% space (mixed local + object storage)
  // ===========================================
  LOG_INFO("--- Scenario 2c: write_dual mode (50% space - mixed local/object) ---");

  const int64_t half_count = segment_count / 2;
  int64_t start_time_write_dual_half = ObTimeUtility::current_time();

  // Phase 1: Write first 50% to local (space is available)
  LOG_INFO("Phase 1: Writing first 50% segments to local cache...");
  for (int64_t i = 0; i < half_count; ++i) {
    MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(1003);
    macro_id.set_third_id(4000 + i);
    macro_id.set_macro_private_transfer_epoch(0);
    macro_id.set_tenant_seq(0);

    write_tmp_file_data(macro_id, 0/*offset*/, segment_size, segment_size/*valid_length*/, true/*is_sealed*/, write_buf_);

    if ((i + 1) % 20 == 0) {
      LOG_INFO("write_dual_half phase1 progress", K(i), K(half_count));
    }
  }
  LOG_INFO("Phase 1 completed: first 50% written to local");

  // Phase 2: Exhaust space, then write remaining 50% to remote
  LOG_INFO("Phase 2: Exhausting space, then writing remaining 50% to remote...");
  int64_t exhausted_size_half = 0;
  exhaust_tmp_file_disk_size(exhausted_size_half);
  LOG_INFO("Exhausted disk space for phase 2", K(exhausted_size_half));

  for (int64_t i = half_count; i < segment_count; ++i) {
    MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(1003);
    macro_id.set_third_id(4000 + i);
    macro_id.set_macro_private_transfer_epoch(0);
    macro_id.set_tenant_seq(0);

    write_tmp_file_data(macro_id, 0/*offset*/, segment_size, segment_size/*valid_length*/, true/*is_sealed*/, write_buf_);

    if ((i + 1) % 20 == 0) {
      LOG_INFO("write_dual_half phase2 progress", K(i), K(segment_count));
    }
  }
  LOG_INFO("Phase 2 completed: remaining 50% written to remote");

  int64_t end_time_write_dual_half = ObTimeUtility::current_time();
  int64_t duration_write_dual_half_us = end_time_write_dual_half - start_time_write_dual_half;
  int64_t duration_write_dual_half_ms = duration_write_dual_half_us / 1000;

  LOG_INFO("Scenario 2c completed", "duration_ms", duration_write_dual_half_ms,
           "duration_us", duration_write_dual_half_us,
           "avg_per_segment_ms", duration_write_dual_half_ms / segment_count);

  release_tmp_file_disk_size(exhausted_size_half);

  // ===========================================
  // Scenario 2d: write_dual mode with 25% local (75% remote)
  // ===========================================
  LOG_INFO("--- Scenario 2d: write_dual mode (25% local / 75% remote) ---");

  const int64_t quarter_count = segment_count / 4;
  int64_t start_time_write_dual_quarter = ObTimeUtility::current_time();

  // Phase 1: Write first 25% to local
  LOG_INFO("Phase 1: Writing first 25% segments to local cache...");
  for (int64_t i = 0; i < quarter_count; ++i) {
    MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(1004);
    macro_id.set_third_id(5000 + i);
    macro_id.set_macro_private_transfer_epoch(0);
    macro_id.set_tenant_seq(0);

    write_tmp_file_data(macro_id, 0/*offset*/, segment_size, segment_size/*valid_length*/, true/*is_sealed*/, write_buf_);

    if ((i + 1) % 20 == 0) {
      LOG_INFO("write_dual_quarter phase1 progress", K(i), K(quarter_count));
    }
  }
  LOG_INFO("Phase 1 completed: first 25% written to local");

  // Phase 2: Exhaust space, then write remaining 75% to remote
  LOG_INFO("Phase 2: Exhausting space, then writing remaining 75% to remote...");
  int64_t exhausted_size_quarter = 0;
  exhaust_tmp_file_disk_size(exhausted_size_quarter);
  LOG_INFO("Exhausted disk space for phase 2", K(exhausted_size_quarter));

  for (int64_t i = quarter_count; i < segment_count; ++i) {
    MacroBlockId macro_id;
    macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(1004);
    macro_id.set_third_id(5000 + i);
    macro_id.set_macro_private_transfer_epoch(0);
    macro_id.set_tenant_seq(0);

    write_tmp_file_data(macro_id, 0/*offset*/, segment_size, segment_size/*valid_length*/, true/*is_sealed*/, write_buf_);

    if ((i + 1) % 20 == 0) {
      LOG_INFO("write_dual_quarter phase2 progress", K(i), K(segment_count));
    }
  }
  LOG_INFO("Phase 2 completed: remaining 75% written to remote");

  int64_t end_time_write_dual_quarter = ObTimeUtility::current_time();
  int64_t duration_write_dual_quarter_us = end_time_write_dual_quarter - start_time_write_dual_quarter;
  int64_t duration_write_dual_quarter_ms = duration_write_dual_quarter_us / 1000;

  LOG_INFO("Scenario 2d completed", "duration_ms", duration_write_dual_quarter_ms,
           "duration_us", duration_write_dual_quarter_us,
           "avg_per_segment_ms", duration_write_dual_quarter_ms / segment_count);

  release_tmp_file_disk_size(exhausted_size_quarter);

  // ===========================================
  // Performance comparison summary
  // ===========================================
  int64_t time_saved_full_ms = duration_write_through_ms - duration_write_dual_full_ms;
  double speedup_ratio_full = static_cast<double>(duration_write_through_us) / static_cast<double>(duration_write_dual_full_us);
  int64_t improvement_percent_full = static_cast<int64_t>((speedup_ratio_full - 1.0) * 100);

  int64_t time_saved_half_ms = duration_write_through_ms - duration_write_dual_half_ms;
  double speedup_ratio_half = static_cast<double>(duration_write_through_us) / static_cast<double>(duration_write_dual_half_us);
  int64_t improvement_percent_half = static_cast<int64_t>((speedup_ratio_half - 1.0) * 100);

  int64_t time_saved_quarter_ms = duration_write_through_ms - duration_write_dual_quarter_ms;
  double speedup_ratio_quarter = static_cast<double>(duration_write_through_us) / static_cast<double>(duration_write_dual_quarter_us);
  int64_t improvement_percent_quarter = static_cast<int64_t>((speedup_ratio_quarter - 1.0) * 100);

  int64_t time_saved_three_quarter_ms = duration_write_through_ms - duration_write_dual_three_quarter_ms;
  double speedup_ratio_three_quarter = static_cast<double>(duration_write_through_us) / static_cast<double>(duration_write_dual_three_quarter_us);
  int64_t improvement_percent_three_quarter = static_cast<int64_t>((speedup_ratio_three_quarter - 1.0) * 100);

  LOG_INFO("=== Performance Comparison Summary ===");
  LOG_INFO("Segment count", K(segment_count));
  LOG_INFO("Segment size (bytes)", K(segment_size));
  LOG_INFO("Scenario 1  (write_through):       total time", "ms", duration_write_through_ms, "us", duration_write_through_us);
  LOG_INFO("Scenario 2a (write_dual - 100% local): total time", "ms", duration_write_dual_full_ms, "us", duration_write_dual_full_us);
  LOG_INFO("Scenario 2b (write_dual - 75% local):  total time", "ms", duration_write_dual_three_quarter_ms, "us", duration_write_dual_three_quarter_us);
  LOG_INFO("Scenario 2c (write_dual - 50% local):  total time", "ms", duration_write_dual_half_ms, "us", duration_write_dual_half_us);
  LOG_INFO("Scenario 2d (write_dual - 25% local):  total time", "ms", duration_write_dual_quarter_ms, "us", duration_write_dual_quarter_us);
  LOG_INFO("Time saved (100% local vs through)", "ms", time_saved_full_ms, "us", duration_write_through_us - duration_write_dual_full_us);
  LOG_INFO("Time saved (75% local vs through)", "ms", time_saved_three_quarter_ms, "us", duration_write_through_us - duration_write_dual_three_quarter_us);
  LOG_INFO("Time saved (50% local vs through)", "ms", time_saved_half_ms, "us", duration_write_through_us - duration_write_dual_half_us);
  LOG_INFO("Time saved (25% local vs through)", "ms", time_saved_quarter_ms, "us", duration_write_through_us - duration_write_dual_quarter_us);
  LOG_INFO("Speedup ratio (100% local)", K(speedup_ratio_full));
  LOG_INFO("Speedup ratio (75% local)", K(speedup_ratio_three_quarter));
  LOG_INFO("Speedup ratio (50% local)", K(speedup_ratio_half));
  LOG_INFO("Speedup ratio (25% local)", K(speedup_ratio_quarter));
  LOG_INFO("Performance improvement (100% local)", "percent", improvement_percent_full);
  LOG_INFO("Performance improvement (75% local)", "percent", improvement_percent_three_quarter);
  LOG_INFO("Performance improvement (50% local)", "percent", improvement_percent_half);
  LOG_INFO("Performance improvement (25% local)", "percent", improvement_percent_quarter);
  LOG_INFO("Average time per segment (write_through)", "ms", duration_write_through_ms / segment_count);
  LOG_INFO("Average time per segment (write_dual_100%)", "ms", duration_write_dual_full_ms / segment_count);
  LOG_INFO("Average time per segment (write_dual_75%)", "ms", duration_write_dual_three_quarter_ms / segment_count);
  LOG_INFO("Average time per segment (write_dual_50%)", "ms", duration_write_dual_half_ms / segment_count);
  LOG_INFO("Average time per segment (write_dual_25%)", "ms", duration_write_dual_quarter_ms / segment_count);

  LOG_INFO("=== Performance Comparison Test Passed ===");
}

TEST_F(TestSSReaderWriter, IOFaultDetector)
{
  ObIOFaultDetector &detector = OB_IO_MANAGER.get_device_health_detector();
  ObIOConfig &io_config = (ObIOConfig &)detector.io_config_;

  // test get device health
  ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
  int64_t disk_abnormal_time = 0;
  ASSERT_EQ(OB_SUCCESS, detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_NORMAL == dhs);
  ASSERT_TRUE(0 == disk_abnormal_time);

  // test read failure detection
  ObIOInfo io_info;
  get_random_io_info(io_info);
  char read_buf[1000] = { 0 };
  io_info.user_data_buf_ = read_buf;
  ObSSPhysicalBlock phy_block;
  ObSSPhyBlockHandle phy_block_handle;
  phy_block_handle.set_ptr(&phy_block);
  ASSERT_EQ(OB_SUCCESS, io_info.phy_block_handle_.assign(phy_block_handle));

  ObIOResult result;
  ObIORequest req;
  req.inc_ref();
  result.inc_ref();
  ASSERT_EQ(OB_SUCCESS, result.basic_init());
  ASSERT_EQ(OB_SUCCESS, result.init(io_info));
  ASSERT_EQ(OB_SUCCESS, req.init(io_info, &result));

  detector.reset_device_health();
  ASSERT_EQ(OB_SUCCESS, detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_NORMAL == dhs);
  ASSERT_TRUE(0 == disk_abnormal_time);
  result.flag_.set_mode(ObIOMode::READ);
  io_config.data_storage_warning_tolerance_time_ = 1000L * 1000L;
  io_config.data_storage_error_tolerance_time_ = 3000L * 1000L;

  detector.record_io_error(result, req);
  usleep(2000L * 1000L);
  ASSERT_FALSE(detector.is_device_warning_);
  ASSERT_FALSE(detector.is_device_error_);

  detector.record_io_timeout(result, req);
  usleep(2000L * 1000L);
  ASSERT_FALSE(detector.is_device_warning_);
  ASSERT_FALSE(detector.is_device_error_);

  // test auto clean device warning, but not clean device error
  detector.reset_device_health();
  ASSERT_EQ(OB_SUCCESS, detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_NORMAL == dhs);
  ASSERT_TRUE(0 == disk_abnormal_time);
  io_config.read_failure_black_list_interval_ = 1000L * 100L; // 100ms
  detector.set_device_warning();
  ASSERT_EQ(OB_SUCCESS, detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_WARNING == dhs);
  ASSERT_TRUE(disk_abnormal_time > 0);
  usleep(io_config.read_failure_black_list_interval_ * 2);
  ASSERT_EQ(OB_SUCCESS, detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_NORMAL == dhs);
  ASSERT_TRUE(0 == disk_abnormal_time);
  detector.set_device_error();
  usleep(io_config.read_failure_black_list_interval_ * 2);
  ASSERT_EQ(OB_SUCCESS, detector.get_device_health_status(dhs, disk_abnormal_time));
  ASSERT_TRUE(DEVICE_HEALTH_ERROR == dhs);
  ASSERT_TRUE(disk_abnormal_time > 0);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_reader_writer.log*");
  OB_LOGGER.set_file_name("test_ss_reader_writer.log", true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
