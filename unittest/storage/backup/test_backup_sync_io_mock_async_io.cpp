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

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/backup/ob_backup_device_wrapper.h"
#include "share/backup/ob_backup_io_adapter.h"


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;
using namespace oceanbase::blocksstable;

#define OK(ass) EXPECT_EQ(OB_SUCCESS, (ass))

namespace oceanbase
{
namespace backup
{

static const int64_t backup_set_id = 1;
static const uint64_t tenant_id = OB_SYS_TENANT_ID;
static const int64_t ls_id = 1001;
static const int64_t turn_id = 1;
static const int64_t retry_id = 0;
static const int64_t file_id = 1024;
static const int64_t offset = 4096;
static const int64_t length = 4096;
static ObBackupDataType backup_data_type;


#define TEST_ROOT_DIR "io_test"
#define TEST_DATA_DIR TEST_ROOT_DIR "/data_dir"
#define TEST_SSTABLE_DIR TEST_DATA_DIR "/sstable"

int init_device(const int64_t media_id, ObLocalDevice &device)
{
  int ret = OB_SUCCESS;
  const int64_t IO_OPT_COUNT = 6;
  const int64_t block_size = 1024L * 1024L * 2L; // 2MB
  const int64_t data_disk_size = 1024L * 1024L * 1024L; // 1GB
  const int64_t data_disk_percentage = 50L;
  ObIODOpt io_opts[IO_OPT_COUNT];
  io_opts[0].key_ = "data_dir";                   io_opts[0].value_.value_str = TEST_DATA_DIR;
  io_opts[1].key_ = "sstable_dir";                io_opts[1].value_.value_str = TEST_SSTABLE_DIR;
  io_opts[2].key_ = "block_size";                 io_opts[2].value_.value_int64 = block_size;
  io_opts[3].key_ = "datafile_disk_percentage";   io_opts[3].value_.value_int64 = data_disk_percentage;
  io_opts[4].key_ = "datafile_size";              io_opts[4].value_.value_int64 = data_disk_size;
  io_opts[5].key_ = "media_id";                   io_opts[5].value_.value_int64 = media_id;
  ObIODOpts init_opts;
  init_opts.opts_ = io_opts;
  init_opts.opt_cnt_ = IO_OPT_COUNT;
  bool need_format = false;
  if (OB_FAIL(device.init(init_opts))) {
    LOG_WARN("init device failed", K(ret));
  } else {
    int64_t reserved_size = 0;
    ObIODOpts opts_start;
    ObIODOpt opt_start;
    opts_start.opts_ = &(opt_start);
    opts_start.opt_cnt_ = 1;
    opt_start.set("reserved size", reserved_size);
    if (OB_FAIL(device.start(opts_start))) {
      LOG_WARN("start device failed", K(ret));
    }
  }
  return ret;
}

class TestBackupMockAsyncIO : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    // prepare test directory and file
    system("mkdir -p " TEST_DATA_DIR);
    system("mkdir -p " TEST_SSTABLE_DIR);

    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  virtual void SetUp()
  {
    ObIOManager::get_instance().destroy();
    const int64_t memory_limit = 10L * 1024L * 1024L * 1024L; // 10GB
    OK(ObIOManager::get_instance().init(memory_limit));
    OK(ObIOManager::get_instance().start());

    // add io device
    OK(OB_IO_MANAGER.add_device_channel(&LOCAL_DEVICE_INSTANCE, 16, 2, 1024));

    static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
    ObTenantEnv::set_tenant(&tenant_ctx);
    ObTenantIOManager *io_service = nullptr;
    OK(ObTenantIOManager::mtl_new(io_service));
    OK(ObTenantIOManager::mtl_init(io_service));
    OK(io_service->start());
    tenant_ctx.set(io_service);
    ObTenantEnv::set_tenant(&tenant_ctx);

    ObMallocAllocator::get_instance()->set_tenant_limit(1, 8L * 1024L * 1024L * 1024L /* 8 GB */);
  }
  virtual void TearDown()
  {
    ObIOManager::get_instance().stop();
    ObIOManager::get_instance().destroy();
  }
};

int prepare_backup_device_macro_block_id(ObBackupDeviceMacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.turn_id_ = turn_id;
  macro_id.length_ = offset;
  macro_id.offset_ = length;
  macro_id.reserved_ = 0;
  macro_id.ls_id_ = ls_id;
  macro_id.backup_set_id_ = backup_set_id;
  macro_id.data_type_ = 1;
  macro_id.retry_id_ = retry_id;
  macro_id.file_id_ = file_id;
  macro_id.block_type_ = ObBackupDeviceMacroBlockId::INDEX_TREE_BLOCK;
  macro_id.id_mode_ = static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_BACKUP);
  macro_id.version_ = ObBackupDeviceMacroBlockId::BACKUP_MACRO_BLOCK_ID_VERSION;
  return ret;
}

int prepare_macro_block_write_info(
    const int64_t offset,
    const char *buf, const int64_t length,
    ObMacroBlockWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  write_info.offset_ = offset;
  write_info.buffer_ = buf;
  write_info.size_ = length;
  return ret;
}

int prepare_macro_block_read_info(
    const ObBackupDeviceMacroBlockId &backup_macro_id,
    const int64_t offset, const int64_t size,
    ObMacroBlockReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_block_id(backup_macro_id.first_id(),
                              backup_macro_id.second_id(),
                              backup_macro_id.third_id());
  read_info.macro_block_id_ = macro_block_id;
  read_info.offset_ = offset;
  read_info.size_ = size;
  read_info.io_desc_.set_mode(ObIOMode::READ);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.io_desc_.set_sys_module_id(ObIOModule::ROOT_BLOCK_IO);
  return ret;
}

int prepare_backup_device_handle(const ObStorageAccessType &access_type,
    ObBackupWrapperIODevice &device, ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  char test_dir[OB_MAX_URI_LENGTH];
  char test_dir_uri[OB_MAX_URI_LENGTH];
  char uri[OB_MAX_URI_LENGTH];

  databuff_printf(test_dir, sizeof(test_dir), "%s/test_backup_async_io_dir", get_current_dir_name());
  databuff_printf(test_dir_uri, sizeof(test_dir_uri), "file://%s", test_dir);
  databuff_printf(uri, sizeof(uri), "file://%s/test_file", test_dir);

  ObBackupStorageInfo storage_info;
  storage_info.device_type_ = ObStorageType::OB_STORAGE_FILE;

  static const int64_t opt_cnt = BACKUP_WRAPPER_DEVICE_OPT_NUM;

  ObIODOpts io_d_opts;
  ObIODOpt opts[opt_cnt];
  io_d_opts.opts_ = opts;
  io_d_opts.opt_cnt_ = opt_cnt;

  backup_data_type.set_user_data_backup();

  io_d_opts.opts_[0].set("storage_info", "");

  const ObBackupDeviceMacroBlockId::BlockType block_type = ObBackupDeviceMacroBlockId::DATA_BLOCK;

  ret = ObBackupWrapperIODevice::setup_io_opts_for_backup_device(
      backup_set_id, ObLSID(ls_id), backup_data_type, turn_id, retry_id, file_id, block_type, access_type, &io_d_opts);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObBackupIoAdapter util;
  ret = util.mkdir(test_dir_uri, &storage_info);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = device.open(uri, -1, 0, io_fd, &io_d_opts);
  EXPECT_EQ(OB_SUCCESS, ret);
  return ret;
}

int prepare_write_io_info(const ObBackupDeviceMacroBlockId &macro_id,
    const ObMacroBlockWriteInfo &write_info, ObIODevice *device_handle, ObIOInfo &write_io_info)
{
  int ret = OB_SUCCESS;
  write_io_info.tenant_id_ = tenant_id;
  write_io_info.fd_.device_handle_ = device_handle;
  write_io_info.fd_.first_id_ = macro_id.first_id();
  write_io_info.fd_.second_id_ = macro_id.second_id();
  write_io_info.fd_.third_id_ = macro_id.third_id();
  write_io_info.offset_ = write_info.offset_;
  write_io_info.size_ = write_info.size_;
  write_io_info.buf_ = write_info.buffer_;
  write_io_info.flag_ = write_info.io_desc_;
  if (ObBackupDeviceMacroBlockId::is_backup_block_file(macro_id.first_id())) {
    write_io_info.fd_.fd_id_ = static_cast<ObBackupWrapperIODevice *>(device_handle)->simulated_fd_id();
    write_io_info.fd_.slot_version_ = static_cast<ObBackupWrapperIODevice *>(device_handle)->simulated_slot_version();
  }
  write_io_info.flag_.set_write();
  return ret;
}

int prepare_read_io_info(const ObBackupDeviceMacroBlockId &macro_id,
    const ObMacroBlockReadInfo &read_info, ObIODevice *device_handle, ObIOInfo &read_io_info)
{
  int ret = OB_SUCCESS;
  read_io_info.tenant_id_ = tenant_id;
  read_io_info.fd_.device_handle_ = device_handle;
  read_io_info.fd_.first_id_ = macro_id.first_id();
  read_io_info.fd_.second_id_ = macro_id.second_id();
  read_io_info.fd_.third_id_ = macro_id.third_id();
  read_io_info.offset_ = read_info.offset_;
  read_io_info.size_ = static_cast<int32_t>(read_info.size_);
  read_io_info.flag_ = read_info.io_desc_;
  char *buf = NULL;
  if (ObBackupDeviceMacroBlockId::is_backup_block_file(macro_id.first_id())) {
    read_io_info.fd_.fd_id_ = static_cast<ObBackupWrapperIODevice *>(device_handle)->simulated_fd_id();
    read_io_info.fd_.slot_version_ = static_cast<ObBackupWrapperIODevice *>(device_handle)->simulated_slot_version();
    if (OB_FAIL(static_cast<ObBackupWrapperIODevice *>(device_handle)->alloc_mem_block(
        read_io_info.size_, buf))) {
      LOG_WARN("failed to alloc mem block", K(ret));
    } else {
      read_io_info.buf_ = buf;
    }
  }
  read_io_info.flag_.set_read();
  return ret;
}

TEST_F(TestBackupMockAsyncIO, test_sync_io_mock_async_io)
{
  OK(ObDeviceManager::get_instance().init_devices_env());

  ObBackupDeviceMacroBlockId macro_id;
  char write_buf[length];
  memset(write_buf, 1, sizeof(write_buf));
  OK(prepare_backup_device_macro_block_id(macro_id));

  ObMacroBlockWriteInfo write_info;
  ObBackupWrapperIODevice write_device_handle;
  ObIOInfo write_io_info;
  ObIOHandle write_io_handle;
  ObIOFd write_io_fd;
  const ObStorageAccessType writer_access_type = OB_STORAGE_ACCESS_APPENDER;

  EXPECT_TRUE(ObBackupDeviceMacroBlockId::is_backup_block_file(macro_id.first_id()));
  // mock write
  OK(prepare_macro_block_write_info(offset, write_buf, length, write_info));
  OK(prepare_backup_device_handle(writer_access_type, write_device_handle, write_io_fd));
  OK(prepare_write_io_info(macro_id, write_info, &write_device_handle, write_io_info));
  OK(ObIOManager::get_instance().aio_write(write_io_info, write_io_handle));
  OK(write_io_handle.wait());

  // TODO(yanfeng): read need MTL, deal with this later
  // ObMacroBlockReadInfo read_info;
  // ObBackupWrapperIODevice read_device_handle;
  // ObIOInfo read_io_info;
  // ObIOHandle read_io_handle;
  // ObIOFd read_io_fd;
  // const ObStorageAccessType read_access_type = OB_STORAGE_ACCESS_READER;

  // char *read_buf = NULL;

  // // mock read
  // OK(prepare_macro_block_read_info(macro_id, offset, length, read_info));
  // OK(prepare_backup_device_handle(read_access_type, read_device_handle, read_io_fd));
  // OK(prepare_read_io_info(macro_id, read_info, &read_device_handle, read_io_info));
  // OK(ObIOManager::get_instance().aio_read(read_io_info, read_io_handle));
  // OK(read_io_handle.wait(wait_timeout));

  // // 比较read_buf和write_buf的内容
  // bool is_same = true;
  // for (int64_t i = 0; i < length; ++i) {
  //   if (read_io_info.buf_[i] != write_buf[i]) {
  //     is_same = false;
  //     break;
  //   }
  // }

  // EXPECT_TRUE(is_same);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_sync_io_mock_async_io.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_sync_io_mock_async_io.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
