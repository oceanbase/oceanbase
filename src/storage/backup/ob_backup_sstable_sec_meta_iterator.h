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

#ifndef STORAGE_BACKUP_SSTABLE_SEC_META_ITERATOR_H_
#define STORAGE_BACKUP_SSTABLE_SEC_META_ITERATOR_H_

#include "common/ob_tablet_id.h"
#include "common/storage/ob_io_device.h"
#include "lib/allocator/page_arena.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_index_store.h"
#include "storage/backup/ob_backup_restore_util.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/ob_i_table.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/meta_mem/ob_tablet_handle.h"

namespace oceanbase {
namespace backup {

class ObBackupSSTableSecMetaIterator final {
public:
  ObBackupSSTableSecMetaIterator();
  ~ObBackupSSTableSecMetaIterator();
  int init(const common::ObTabletID &tablet_id,
           const storage::ObTabletHandle &tablet_handle,
           const storage::ObITable::TableKey &table_key,
           const share::ObBackupDest &backup_dest,
           const share::ObBackupSetDesc &backup_set_desc,
           ObBackupMetaIndexStore &meta_index_store);
  int get_next(blocksstable::ObDataMacroBlockMeta &macro_meta);

private:
  int get_backup_data_type_(const storage::ObITable::TableKey &table_key,
                            share::ObBackupDataType &backup_data_type);
  int get_meta_index_(const common::ObTabletID &tablet_id,
                      ObBackupMetaIndexStore &meta_index_store,
                      ObBackupMetaIndex &meta_index);
  int get_backup_data_path_(const share::ObBackupDest &backup_dest,
                            const share::ObBackupSetDesc &backup_set_desc,
                            const share::ObBackupDataType &backup_data_type,
                            const ObBackupMetaIndex &meta_index,
                            share::ObBackupPath &backup_path);
  int read_backup_sstable_metas_(
      const share::ObBackupDest &backup_dest,
      const share::ObBackupPath &backup_path,
      const ObBackupMetaIndex &meta_index,
      common::ObIArray<ObBackupSSTableMeta> &sstable_meta_array);
  int get_backup_sstable_meta_ptr_(
      const storage::ObITable::TableKey &table_key,
      common::ObArray<ObBackupSSTableMeta> &backup_metas,
      ObBackupSSTableMeta *&ptr);

private:
  int build_create_sstable_param_(
      const storage::ObTabletHandle &tablet_handle,
      const ObBackupSSTableMeta &backup_sstable_meta,
      ObTabletCreateSSTableParam &param);
  int build_create_none_empty_sstable_param_(
      const ObBackupSSTableMeta &backup_sstable_meta,
      ObTabletCreateSSTableParam &param);
  int build_create_empty_sstable_param_(
      const storage::ObTabletHandle &tablet_handle,
      const ObBackupSSTableMeta &backup_sstable_meta,
      ObTabletCreateSSTableParam &param);
  int create_tmp_sstable_(const ObTabletCreateSSTableParam &param);
  int init_sstable_sec_meta_iter_(const ObTabletHandle &tablet_handle);

private:
  bool is_inited_;
  int64_t output_idx_;
  common::ObTabletID tablet_id_;
  storage::ObITable::TableKey table_key_;
  common::ObArenaAllocator allocator_;
  ObTableHandleV2 table_handle_;
  blocksstable::ObDatumRange datum_range_;
  blocksstable::ObSSTableSecMetaIterator sec_meta_iterator_;
  // TODO: yangyi.yyy, 适配mod
  common::ObStorageIdMod mod_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupSSTableSecMetaIterator);
};

} // namespace backup
} // namespace oceanbase

#endif
