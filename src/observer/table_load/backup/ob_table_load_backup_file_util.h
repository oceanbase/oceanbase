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
};

} // namespace observer
} // namespace oceanbase
