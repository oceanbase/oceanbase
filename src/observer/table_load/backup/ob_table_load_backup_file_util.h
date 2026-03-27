/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once
#include "share/backup/ob_backup_struct.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace observer
{

class ObTableLoadBackupFileUtil
{
public:
  static const int64_t MAX_RETRY_COUNT = 12; // retry 1 minute
  static const int64_t RETRY_INTERVAL = 5 * 1000 * 1000; // 5s
  ObTableLoadBackupFileUtil() {}
  ~ObTableLoadBackupFileUtil() {}
  static int list_directories(const common::ObString &path,
                              const share::ObBackupStorageInfo *storage_info,
                              ObIArray<ObString> &part_list_,
                              ObIAllocator &allocator);
  static int get_file_length(const common::ObString &path,
                             const share::ObBackupStorageInfo *storage_info,
                             int64_t &file_length);
  static int read_single_file(const common::ObString &path,
                              const share::ObBackupStorageInfo *storage_info,
                              char *buf,
                              const int64_t buf_size,
                              int64_t &read_size);
  static int read_part_file(const common::ObString &path,
                            const share::ObBackupStorageInfo *storage_info,
                            char *buf,
                            const int64_t buf_size,
                            const int64_t offset,
                            int64_t &read_size);
  static int split_reverse(ObString &str,
                           const char separator,
                           ObIArray<ObString> &result,
                           int64_t limit = -1,
                           bool ignore_empty = false);
};

} // namespace observer
} // namespace oceanbase
