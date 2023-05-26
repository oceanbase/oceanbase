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

#ifndef STORAGE_LOG_STREAM_BACKUP_RESTORE_UTIL_H_
#define STORAGE_LOG_STREAM_BACKUP_RESTORE_UTIL_H_

#include "storage/tablet/ob_tablet_meta.h"
#include "common/ob_tablet_id.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "share/backup/ob_backup_path.h"

namespace oceanbase {
namespace backup {

class ObLSBackupRestoreUtil {
public:
  static int read_tablet_meta(const common::ObString &path, const share::ObBackupStorageInfo *storage_info,
      const share::ObBackupDataType &backup_data_type, const ObBackupMetaIndex &meta_index, ObBackupTabletMeta &tablet_meta);
  static int read_sstable_metas(const common::ObString &path, const share::ObBackupStorageInfo *storage_info,
      const ObBackupMetaIndex &meta_index, common::ObIArray<ObBackupSSTableMeta> &sstable_metas);
  static int read_macro_block_id_mapping_metas(const common::ObString &path, const share::ObBackupStorageInfo *storage_info,
      const ObBackupMetaIndex &meta_index, ObBackupMacroBlockIDMappingsMeta &id_mappings_meta);
  static int read_macro_block_data(const common::ObString &path, const share::ObBackupStorageInfo *storage_info,
      const ObBackupMacroBlockIndex &macro_index, const int64_t align_size, blocksstable::ObBufferReader &read_buffer,
      blocksstable::ObBufferReader &data_buffer);
  static int pread_file(
      const ObString &path, const share::ObBackupStorageInfo *storage_info, const int64_t offset, const int64_t read_size, char *buf);
};

}  // namespace backup
}  // namespace oceanbase

#endif
