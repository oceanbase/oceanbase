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
#include "common/storage/ob_device_common.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_linked_item.h"
#include "storage/backup/ob_backup_linked_block_reader.h"

namespace oceanbase {
namespace backup {

class ObBackupMetaKVCache;

class ObLSBackupRestoreUtil {
public:
  static int read_tablet_meta(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const common::ObStorageIdMod &mod,
      const ObBackupMetaIndex &meta_index, ObBackupTabletMeta &tablet_meta);
  static int read_sstable_metas(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const common::ObStorageIdMod &mod,
      const ObBackupMetaIndex &meta_index, ObBackupMetaKVCache *kv_cache, common::ObIArray<ObBackupSSTableMeta> &sstable_metas);
  static int read_macro_block_id_mapping_metas(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const common::ObStorageIdMod &mod, 
      const ObBackupMetaIndex &meta_index, ObBackupMacroBlockIDMappingsMeta &id_mappings_meta);
  static int read_macro_block_data(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const common::ObStorageIdMod &mod,
      const ObBackupMacroBlockIndex &macro_index, const int64_t align_size, blocksstable::ObBufferReader &read_buffer, 
      blocksstable::ObBufferReader &data_buffer);
  static int read_macro_block_data_with_retry(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const common::ObStorageIdMod &mod,
      const ObBackupMacroBlockIndex &macro_index, const int64_t align_size, blocksstable::ObBufferReader &read_buffer, 
      blocksstable::ObBufferReader &data_buffer); // max retry count is GCONF._restore_io_max_retry_count
  static int read_ddl_sstable_other_block_id_list_in_ss_mode(
      const share::ObBackupDest &backup_set_dest, const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const ObStorageIdMod &mod,
      const ObBackupMetaIndex &meta_index, const storage::ObITable::TableKey &table_key, common::ObIArray<ObBackupLinkedItem> &link_item);
  static int read_ddl_sstable_other_block_id_list_in_ss_mode_with_batch(
      const share::ObBackupDest &backup_set_dest, const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const ObStorageIdMod &mod,
      const ObBackupMetaIndex &meta_index, const storage::ObITable::TableKey &table_key, const blocksstable::MacroBlockId &start_macro_id,
      const int64_t count, common::ObIArray<ObBackupLinkedItem> &link_item);
  static int pread_file(const ObString &path, const share::ObBackupStorageInfo *storage_info, const common::ObStorageIdMod &mod,
      const int64_t offset, const int64_t read_size, char *buf);

private:
  static int prepare_ddl_sstable_other_block_id_reader_(
      const share::ObBackupDest &backup_set_dest, const common::ObString &path, const share::ObBackupStorageInfo *storage_info, const ObStorageIdMod &mod,
      const ObBackupMetaIndex &meta_index, const storage::ObITable::TableKey &table_key, ObBackupLinkedBlockItemReader &reader, bool &has_any_block);
  static const int64_t READ_MACRO_BLOCK_RETRY_INTERVAL = 1 * 1000 * 1000LL; // 1s      
};

}  // namespace backup
}  // namespace oceanbase

#endif
