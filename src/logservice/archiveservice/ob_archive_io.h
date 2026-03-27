/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/string/ob_string.h"      // ObString
#include "share/backup/ob_backup_struct.h"

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_IO_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_IO_H_

namespace oceanbase
{
namespace archive
{
using oceanbase::common::ObString;
class ObArchiveIO
{
public:
  ObArchiveIO() {}
  ~ObArchiveIO() {}

public:
  int push_log(const ObString &uri,
      const share::ObBackupStorageInfo *storage_info,
      const int64_t backup_dest_id,
      char *data,
      const int64_t data_len,
      const int64_t offset,
      const bool is_full_file,
      const bool is_can_seal);

  int mkdir(const ObString &uri,
      const share::ObBackupStorageInfo *storage_info);

private:
  int check_context_match_in_normal_file_(const ObString &uri,
      const share::ObBackupStorageInfo *storage_info,
      const common::ObStorageIdMod &storage_id_mod,
      char *data,
      const int64_t data_len,
      const int64_t offset);
};
} // namespace archive
} // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_IO_H_ */
