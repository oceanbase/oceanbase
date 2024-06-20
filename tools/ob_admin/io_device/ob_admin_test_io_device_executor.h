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
#include "../ob_admin_executor.h"
#include "deps/oblib/src/lib/ob_define.h"                    // OB_MAX_URI_LENGTH
#include "share/backup/ob_backup_struct.h"                  // OB_MAX_BACKUP_STORAGE_INFO_LENGTH

namespace oceanbase {
namespace tools {

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
  int generate_random_str_(char *&buf, const int64_t size);
  int print_usage_();

private:
  char backup_path_[common::OB_MAX_URI_LENGTH];
  char storage_info_[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  bool is_quiet_;
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

  DISALLOW_COPY_AND_ASSIGN(ObAdminTestIODeviceExecutor);
};

} //namespace tools
} //namespace oceanbase

#endif