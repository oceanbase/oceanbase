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

#ifndef OB_ADMIN_TEST_IO_DEVICE_EXECUTOR_H_
#define OB_ADMIN_TEST_IO_DEVICE_EXECUTOR_H_
#include <gtest/gtest.h>
#include "share/backup/ob_backup_io_adapter.h"
#include "../ob_admin_executor.h"
#include "deps/oblib/src/lib/ob_define.h"                    // OB_MAX_URI_LENGTH
#include "share/backup/ob_backup_struct.h"                  // OB_MAX_BACKUP_STORAGE_INFO_LENGTH
#include "../dumpsst/ob_admin_dumpsst_print_helper.h"

namespace oceanbase {
namespace tools {

#define STORAGE_LOG_FILTER(level, info_string, args...) \
  STORAGE_LOG(level, info_string, ##args); \
  if (strcmp(#level, "ERROR") == 0) {                  \
    std::cout << LIGHT_RED << "ERROR INFO: " << info_string << NONE_COLOR << std::endl;  \
  }

struct TestObjectStorageInterfaceContext
{
  TestObjectStorageInterfaceContext();

  ObBackupIoAdapter util_;
  share::ObBackupStorageInfo storage_info_;

  const char *test_dir_name_ = "test_dir_path";
  const char *single_file_name_ = "single_file.txt";
  const char *appendable_file_name_ = "appendable_file.txt";
  const char *upload_file_name_ = "upload_file.txt";
  const int64_t SINGLE_FILE_LENGTH = 2L * 1024 * 1024;
  const int64_t APPENDABLE_FILE_LENGTH = 32L * 1024 * 1024;
  const int64_t UPLOAD_FILE_LENGTH = 6L * 1024 * 1024;
  const int64_t S3_FILE_COUNT = 10;
  const int64_t OTHER_FILE_COUNT = 3;
  char *single_file_content_;
  char *appendable_file_content_;
  char *upload_file_content_;
  bool single_file_is_exist_;
  bool appendable_file_is_exist_;
  bool upload_file_is_exist_;
  char test_dir_path_[OB_MAX_URI_LENGTH];
  char single_file_path_[OB_MAX_URI_LENGTH];
  char appendable_file_path_[OB_MAX_URI_LENGTH];
  char upload_file_path_[OB_MAX_URI_LENGTH];
};

class TestExecGuard
{
public:
  TestExecGuard(const char *title, int64_t &test_id, int &ret);
  ~TestExecGuard();

private:
  const char *title_;
  int64_t &test_id_;
  int &ret_;
  const int64_t start_time_us_;
};

class ObAdminTestIODeviceExecutor : public ObAdminExecutor
{
public:
  ObAdminTestIODeviceExecutor();
  virtual ~ObAdminTestIODeviceExecutor();
  virtual int execute(int argc, char *argv[]) override;

private:
  int parse_cmd_(int argc, char *argv[]);
  int run_all_tests_();
  int test_backup_dest_connectivity_();
  int test_normal_check_file_(const char* check_file_dir);
  int test_appendable_check_file_(const char* check_file_dir);
  int test_multipart_upload_check_file_(const char* check_file_dir);
  int test_clean_backup_file_();
  int test_backup_data_();
  int test_archive_log_();
  int test_full_clog_file_();
  int test_partial_clog_file_();
  int test_consume_clog_file_();
  int test_restore_data_();
  int test_object_storage_interface_();
  int test_object_storage_interface_prepare_(TestObjectStorageInterfaceContext &ctx);
  int test_object_storage_interface_async_upload_(TestObjectStorageInterfaceContext &ctx);
  int test_object_storage_interface_is_exist_(TestObjectStorageInterfaceContext &ctx);
  int test_object_storage_interface_list_file_(TestObjectStorageInterfaceContext &ctx);
  int test_object_storage_interface_list_directories_(TestObjectStorageInterfaceContext &ctx);
  int test_object_storage_interface_get_file_length_(TestObjectStorageInterfaceContext &ctx);
  int test_object_storage_interface_read_(TestObjectStorageInterfaceContext &ctx);
  int test_object_storage_interface_del_(TestObjectStorageInterfaceContext &ctx);
  int test_multi_step_write_appendable_file_(TestObjectStorageInterfaceContext &ctx);
  int test_list_before_complete_multipart_write_();
  int generate_random_str_(char *&buf, const int64_t size);
  int print_usage_();
  bool is_simulate_append_(TestObjectStorageInterfaceContext &ctx) const;

private:
  char backup_path_[common::OB_MAX_URI_LENGTH];
  char storage_info_[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  bool is_quiet_;
  int64_t test_id_;
  common::ObArenaAllocator allocator_;

private:
  static constexpr char* HELP_FMT = const_cast<char*>("\t%-30s%-12s\n");
  static const int64_t MIN_PART_SIZE = 1024 * 1024;
  static const int64_t MAX_PART_SIZE = 2 * 1024 * 1024;
  static const int64_t MIN_APPEND_SIZE = 5 * 1024 * 1024;
  static const int64_t MAX_APPEND_SIZE = 10 * 1024 * 1024;
  static const int64_t UPLOAD_TIMES = 10;
  static const int64_t APPEND_TIMES = 5;
  static const int64_t CLOG_FILE_SIZE = 64 * 1024 * 1024;

  static const int64_t IO_TIMEOUT_MS = 120LL * 1000LL;

  DISALLOW_COPY_AND_ASSIGN(ObAdminTestIODeviceExecutor);
};

} //namespace tools
} //namespace oceanbase

#endif
