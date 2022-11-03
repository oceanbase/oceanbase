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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_FILE_UTILS_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_FILE_UTILS_H_

#include "lib/string/ob_string.h"
#include "share/backup/ob_backup_struct.h"
namespace oceanbase
{
namespace archive
{
using oceanbase::common::ObString;
class ObArchiveFileUtils
{
public:
  static int get_file_range(const ObString &prifix,
      const share::ObBackupStorageInfo *storage_info,
      int64_t &min_file_id,
      int64_t &max_file_id);

  static int read_file(const ObString &uri,
      const share::ObBackupStorageInfo *storage_info,
      char *buf,
      const int64_t file_len,
      int64_t &read_size);

  static int range_read(const ObString &uri,
      const share::ObBackupStorageInfo *storage_info,
      char *buf,
      const int64_t buf_size,
      const int64_t offset,
      int64_t &read_size);

  static int get_file_length(const ObString &uri,
      const share::ObBackupStorageInfo *storage_info,
      int64_t &file_length);

  static int extract_file_id(const ObString &file_name, int64_t &file_id, bool &match);
};
} // namespace archive
} // namespace
#endif
