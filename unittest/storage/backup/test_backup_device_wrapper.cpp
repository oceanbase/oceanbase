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
#include "storage/backup/ob_backup_device_wrapper.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_device_manager.h"
#define private public
#define protected public

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))

namespace oceanbase
{
namespace backup
{

TEST(TestBackupDeviceWrapper, test_alloc_block) {
  char test_dir[OB_MAX_URI_LENGTH];
  char test_dir_uri[OB_MAX_URI_LENGTH];
  char uri[OB_MAX_URI_LENGTH];

  OK(databuff_printf(test_dir, sizeof(test_dir), "%s/test_backup_device_wrapper_dir", get_current_dir_name()));
  OK(databuff_printf(test_dir_uri, sizeof(test_dir_uri), "file://%s", test_dir));
  OK(databuff_printf(uri, sizeof(uri), "file://%s/test_file", test_dir));

  ObBackupStorageInfo storage_info;
  storage_info.device_type_ = ObStorageType::OB_STORAGE_FILE;

  const int64_t backup_set_id = 1;
  const share::ObLSID ls_id(1001);
  share::ObBackupDataType backup_data_type;
  backup_data_type.set_user_data_backup();
  const int64_t turn_id = 1;
  const int64_t retry_id = 0;
  const int64_t file_id = 0;
  const ObBackupDeviceMacroBlockId::BlockType block_type = ObBackupDeviceMacroBlockId::INDEX_TREE_BLOCK;

  static const int64_t opt_cnt = BACKUP_WRAPPER_DEVICE_OPT_NUM;

  ObIODOpts io_d_opts_write;
  ObIODOpt write_opts[opt_cnt];
  io_d_opts_write.opts_ = write_opts;
  io_d_opts_write.opt_cnt_ = opt_cnt;

  io_d_opts_write.opts_[0].set("storage_info", "");

  const uint64_t test_memory = 6L * 1024L * 1024L * 1024L;
  OK(ObDeviceManager::get_instance().init_devices_env());
  OK(ObIOManager::get_instance().init(test_memory));

  OK(ObBackupWrapperIODevice::setup_io_opts_for_backup_device(
      backup_set_id, ls_id, backup_data_type, turn_id, retry_id, file_id, block_type, OB_STORAGE_ACCESS_APPENDER, &io_d_opts_write));

  ObBackupWrapperIODevice wrapper_io_write;

  ObBackupIoAdapter util;
  OK(util.mkdir(test_dir_uri, &storage_info));

  ObIOFd write_io_fd;
  OK(wrapper_io_write.open(uri, -1, 0, write_io_fd, &io_d_opts_write));

  // Allocate a block
  ObIOFd block_id;
  OK(wrapper_io_write.alloc_block(&io_d_opts_write, block_id));

  write_io_fd.first_id_ = block_id.first_id_;
  write_io_fd.second_id_ = block_id.second_id_;
  write_io_fd.third_id_ = block_id.third_id_;

  const int64_t size = 100;
  char write_buf[size];
  memset(write_buf, 1, sizeof(write_buf));
  int64_t write_size = 0;
  OK(wrapper_io_write.pwrite(write_io_fd, 0, size, write_buf, write_size));
  EXPECT_EQ(write_size, size);

  ObIODOpts io_d_opts_read;
  ObIODOpt read_opts[opt_cnt];
  io_d_opts_read.opts_ = read_opts;
  io_d_opts_read.opt_cnt_ = opt_cnt;

  io_d_opts_read.opts_[0].set("storage_info", "");

  OK(ObBackupWrapperIODevice::setup_io_opts_for_backup_device(
      backup_set_id, ls_id, backup_data_type, turn_id, retry_id, file_id, block_type, OB_STORAGE_ACCESS_READER, &io_d_opts_read));

  ObBackupWrapperIODevice wrapper_io_read;
  ObIOFd read_io_fd;
  OK(wrapper_io_read.open(uri, -1, 0, read_io_fd, &io_d_opts_read));

  read_io_fd.first_id_ = block_id.first_id_;
  read_io_fd.second_id_ = block_id.second_id_;
  read_io_fd.third_id_ = block_id.third_id_;

  char read_buf[size];
  int64_t read_size = 0;
  OK(wrapper_io_read.pread(read_io_fd, 0, size, read_buf, read_size, NULL));
  EXPECT_EQ(write_size, read_size);

  // 比较read_buf和write_buf的内容
  bool is_same = true;
  for (int64_t i = 0; i < size; ++i) {
    if (read_buf[i] != write_buf[i]) {
      is_same = false;
      break;
    }
  }

  EXPECT_TRUE(is_same);

}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_device_wrapper.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_device_wrapper.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
