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

#ifndef OB_ADMIN_SLOG_EXECUTOR_H_
#define OB_ADMIN_SLOG_EXECUTOR_H_

#include "../ob_admin_executor.h"
#include "storage/blocksstable/ob_log_file_spec.h"
#include <dirent.h>
#include "common/storage/ob_device_common.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace tools
{

class ObLogDirEntryOperation : public ObBaseDirEntryOperator
{
public:
  ObLogDirEntryOperation() {};
  virtual ~ObLogDirEntryOperation();
  virtual int func(const dirent *entry) override;
  void reset();

public:
  static const int64_t MAX_TENANT_NUM = 64;
  static const int64_t MAX_D_NAME_LEN = 256;

public:
  int64_t size_;
  char d_names_[MAX_TENANT_NUM][MAX_D_NAME_LEN];
};

class ObAdminSlogExecutor : public ObAdminExecutor
{
public:
  ObAdminSlogExecutor();
  virtual ~ObAdminSlogExecutor() = default;
  virtual int execute(int argc, char *argv[]);

private:
  int parse_args(int argc, char *argv[]);
  void print_usage();
  int scan_periodically();
  int concat_dir(const char *root_dir, const char *dir);
  int parse_log(const char *slog_dir, const int64_t file_id);

private:
  static const int64_t FILE_MAX_SIZE = 256 << 20;
  uint64_t tenant_id_;
  int64_t log_file_id_;
  bool period_scan_;
  ObLogDirEntryOperation dir_op_;
  int64_t offset_;
  int64_t parse_count_;
  char slog_dir_[common::MAX_PATH_SIZE];
};

}
}

#endif