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
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_meta_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id, transfer_seq));

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
  ObSSLocalCacheTabletStatEntry entry;
  local_cache_service->get_local_cache_tablet_stat(effective_tablet_id, entry);
  ASSERT_EQ(access_cnt, entry.access_cnt_);
  ASSERT_EQ(access_size, entry.access_size_);
  ASSERT_EQ(hit_cnt, entry.hit_cnt_);
  ASSERT_EQ(hit_size, entry.hit_size_);
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
  ASSERT_EQ(read_cnt, type_stat.read_cnt_);
  ASSERT_EQ(read_size, type_stat.read_size_);
  ASSERT_EQ(write_cnt, type_stat.write_cnt_);
  ASSERT_EQ(write_size, type_stat.write_size_);
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
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*trasfer_seq*/));

  // 1. write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(100); // seq_id
  macro_id.set_macro_transfer_seq(0); // transfer_seq
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
  // (c) append sealed segment, [0, 2MB], write remote; expect delete tmp_file_seg_meta
  release_tmp_file_disk_size(avail_size);
  check_tmp_file_disk_size_enough(8192);
  macro_id.set_third_id(72); // segment_id
  write_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/, 8192/*valid_length*/, false/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, 8192/*size*/);
  check_tmp_file_seg_meta(macro_id, true/*is_meta_exist*/, true/*is_in_local*/, 8192/*valid_length*/);

  file_manager->set_tmp_file_cache_pause_gc();

  write_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*valid_length*/, true/*is_sealed*/, write_buf_);
  read_and_compare_tmp_file_data(macro_id, 0/*offset*/, OB_DEFAULT_MACRO_BLOCK_SIZE/*size*/);
  check_tmp_file_seg_meta(macro_id, false/*is_meta_exist*/);

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
  int64_t transfer_seq = 0;
  int64_t version_id = 1;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_ls_id_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_meta_tablet_id_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_meta_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id, transfer_seq));

  // 1. write to local cache
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_TABLET_META);
  macro_id.set_second_id(ls_id);
  macro_id.set_third_id(tablet_id);
  macro_id.set_meta_transfer_seq(transfer_seq);
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

  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*trasfer_seq*/));

  // 1. write 4KB
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(900); // seq_id
  macro_id.set_macro_transfer_seq(0); // transfer_seq
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
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_meta_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id, transfer_seq));

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
