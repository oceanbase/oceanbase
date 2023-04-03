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

#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/string/ob_string.h"
#include "share/backup/ob_backup_struct.h"
#include <cstdint>
namespace oceanbase
{
namespace archive
{
using oceanbase::common::ObString;
typedef common::ObArray<int64_t> ArchiveFileIdArray;
class ObArchiveFileUtils
{
public:
  static int get_file_range(const ObString &prifix,
      const share::ObBackupStorageInfo *storage_info,
      int64_t &min_file_id,
      int64_t &max_file_id);

  static int list_files(const ObString &prifix,
      const share::ObBackupStorageInfo *storage_info,
      common::ObIArray<int64_t> &array);

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

  static int write_file(const ObString &uri,
      const share::ObBackupStorageInfo *storage_info,
      const char *buf,
      const int64_t buf_len);

  static int is_exist(const ObString &uri,
      const share::ObBackupStorageInfo *storage_info,
      bool &exist);

  static int mkdir(const ObString &uri, const share::ObBackupStorageInfo *storage_info);

  static int get_file_length(const ObString &uri,
      const share::ObBackupStorageInfo *storage_info,
      int64_t &file_length);

  static int extract_file_id(const ObString &file_name, int64_t &file_id, bool &match);

  static int locate_file_by_scn(share::ObBackupDest &dest,
      const int64_t dest_id,
      const int64_t round,
      const int64_t piece_id,
      const share::ObLSID &ls_id,
      const share::SCN &scn,
      int64_t &file_id);

  static int locate_file_by_scn_in_piece(share::ObBackupDest &dest,
      const int64_t dest_id,
      const int64_t round,
      const int64_t piece_id,
      const share::ObLSID &ls_id,
      const int64_t min_file_id,
      const int64_t max_file_id,
      const share::SCN &scn,
      int64_t &file_id);
private:

  static int extract_file_min_log_info_(const ObString &uri,
      share::ObBackupStorageInfo *storage_info,
      palf::LSN &lsn,
      share::SCN &scn);

  static int locate_(const int64_t min_file_id,
      const int64_t max_file_id,
      const share::SCN &ref_scn,int64_t &file_id,
      const std::function<int (const int64_t file_id, share::SCN &scn_param, palf::LSN &lsn)> &);
};
} // namespace archive
} // namespace
#endif
