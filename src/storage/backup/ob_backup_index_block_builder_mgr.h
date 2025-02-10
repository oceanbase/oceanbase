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

#ifndef STORAGE_LOG_STREAM_BACKUP_INDEX_BLOCK_BUILDER_MGR_H_
#define STORAGE_LOG_STREAM_BACKUP_INDEX_BLOCK_BUILDER_MGR_H_

#include "share/ob_ls_id.h"
#include "share/backup/ob_backup_struct.h"
#include "common/ob_tablet_id.h"
#include "storage/ob_i_table.h"
#include "storage/backup/ob_backup_utils.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

namespace oceanbase
{
namespace backup
{

class ObBackupTabletIndexBlockBuilderMgr;
class ObBackupProviderItem;

// this class is used to manage index block rebuilder for each backup task
class ObBackupTaskIndexRebuilderMgr final
{
public:
  ObBackupTaskIndexRebuilderMgr();
  ~ObBackupTaskIndexRebuilderMgr();

  void reset();
  int init(const common::ObIArray<ObBackupProviderItem> &item_list,
      ObBackupTabletIndexBlockBuilderMgr *index_builder_mgr, common::ObIArray<ObIODevice *> &device_handle_array);
  int prepare_index_block_rebuilder_if_need(const ObBackupProviderItem &item, const int64_t *task_idx, bool &is_opened);
  int get_index_block_rebuilder(const storage::ObITable::TableKey &table_key, blocksstable::ObIndexBlockRebuilder *&rebuilder);
  int close_index_block_rebuilder_if_need(const ObBackupProviderItem &item, bool &is_closed);
  int check_is_last_item_for_table_key(const ObBackupProviderItem &item, bool &is_last);

private:
  int prepare_index_block_rebuilder_(const ObBackupProviderItem &item, const int64_t *task_idx,
      common::ObIArray<ObIODevice *> &device_handle_array, blocksstable::ObIndexBlockRebuilder *&rebuilder);
  int get_sstable_index_builder_ptr_(const common::ObTabletID &tablet_id,
      const storage::ObITable::TableKey &table_key, blocksstable::ObSSTableIndexBuilder *&sstable_index_builder);
  int check_is_first_item_for_table_key_(const ObBackupProviderItem &item, bool &is_first);
  int check_is_last_item_for_table_key_(const ObBackupProviderItem &item, bool &is_last);

private:
  bool is_inited_;
  common::ObArray<ObBackupProviderItem> item_list_;
  ObBackupTabletIndexBlockBuilderMgr *index_builder_mgr_;
  ObArray<ObIODevice *> device_handle_array_;
  storage::ObITable::TableKey cur_table_key_;
  blocksstable::ObIndexBlockRebuilder *index_block_rebuilder_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTaskIndexRebuilderMgr);
};

// this class is used to manage the index block builder. Each concurrent thread that backs up data has a rebuilder, but only one sstable has an index builder.
// The builder::close function should be called only when all rebuilders are closed.
// To handle this uncertainty, the builder instance is maintained in memory until builder::close is called.
// After calling builder::close, the instance is removed from memory.
// When closing the builder, it's important to pass the device handle so that the builder can dump the index tree to backup device.

class ObBackupTabletSSTableIndexBuilderMgr;

class ObBackupTabletIndexBlockBuilderMgr final
{
public:
  ObBackupTabletIndexBlockBuilderMgr();
  ~ObBackupTabletIndexBlockBuilderMgr();
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id);
  void reset();
  void reuse();
  int check_and_prepare_sstable_index_builders(ObTabletHandle &tablet_handle, const bool is_major_compaction_mview_dep_tablet,
      const common::ObIArray<ObSSTableWrapper> &sstable_array, const common::ObTabletID &tablet_id);
  int get_sstable_index_builder_mgr(const common::ObTabletID &tablet_id, ObBackupTabletSSTableIndexBuilderMgr *&builder_mgr);
  int check_sstable_index_builder_mgr_exist(const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key, bool &exist);
  int get_sstable_index_builder(const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key,
      blocksstable::ObSSTableIndexBuilder *&sstable_index_builder);
  int get_sstable_merge_result(const common::ObTabletID &tablet_id,
      const storage::ObITable::TableKey &table_key, blocksstable::ObSSTableMergeRes *&merge_res);
  int close_sstable_index_builder(const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key,
      ObIODevice *device_handle);
  int free_sstable_index_builder(const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key);
  int remove_sstable_index_builder(const common::ObTabletID &tablet_id);

private:
  int check_sstable_index_builder_mgr_exist_without_lock_(
      const common::ObTabletID &tablet_id, const storage::ObITable::TableKey &table_key, bool &exist);
  int prepare_sstable_index_builders_without_lock_(const common::ObTabletID &tablet_id,
      const common::ObIArray<storage::ObITable::TableKey> &table_keys,
      const bool is_major_compaction_mview_dep_tablet);
  int open_sstable_index_builders_without_lock_(const common::ObTabletID &tablet_id, const ObTabletHandle &tablet_handle,
      const common::ObIArray<storage::ObSSTableWrapper> &sstable_array);

private:
  typedef common::hash::ObHashMap<common::ObTabletID, ObBackupTabletSSTableIndexBuilderMgr *> TabletSSTableBuilderMap;
  static const int64_t BUCKET_NUM = 2048;

private:
  bool is_inited_;
  lib::ObMutex mutex_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  TabletSSTableBuilderMap sstable_builder_map_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletIndexBlockBuilderMgr);
};

struct ObLSBackupCtx;

class ObBackupTabletSSTableIndexBuilderMgr final
{
public:
  ObBackupTabletSSTableIndexBuilderMgr();
  ~ObBackupTabletSSTableIndexBuilderMgr();
  void reset();
  int init(const uint64_t tenant_id, const common::ObTabletID &tablet_id,
      const common::ObIArray<storage::ObITable::TableKey> &table_key_array,
      const bool is_major_compaction_mview_dep_tablet);
  int add_sstable_index_builder(const share::ObLSID &ls_id, const ObTabletHandle &tablet_handle,
      const storage::ObITable::TableKey &table_key, blocksstable::ObSSTable *sstable);
  int get_sstable_index_builder(const storage::ObITable::TableKey &table_key,
      blocksstable::ObSSTableIndexBuilder *&index_builder);
  int get_sstable_merge_result(const storage::ObITable::TableKey &table_key, blocksstable::ObSSTableMergeRes *&merge_res);
  int get_sstable_index_builders(common::ObIArray<blocksstable::ObSSTableIndexBuilder *> &builders);
  int free_sstable_index_builder(const storage::ObITable::TableKey &table_key);
  int close_sstable_index_builder(const storage::ObITable::TableKey &table_key, ObIODevice *device_handle);
  bool is_major_compaction_mview_dep_tablet() const { return is_major_compaction_mview_dep_tablet_; }
  TO_STRING_KV(K_(tablet_id), K_(table_keys), K_(builders), K_(merge_results));

private:
  int get_merge_type_(const storage::ObITable::TableKey &table_key, compaction::ObMergeType &merge_type);
  int prepare_data_store_desc_(const share::ObLSID &ls_id, const ObTabletHandle &tablet_handle,
      const storage::ObITable::TableKey &table_key, blocksstable::ObSSTable *sstable,
      blocksstable::ObWholeDataStoreDesc &data_store_desc);
  int get_table_key_idx_(const storage::ObITable::TableKey &table_key, int64_t &idx);
  int alloc_sstable_index_builder_(const storage::ObITable::TableKey &table_key,
      const blocksstable::ObDataStoreDesc &data_store_desc, blocksstable::ObSSTableIndexBuilder *&index_builder);
  int close_sstable_index_builder_(blocksstable::ObSSTableIndexBuilder *index_builder,
      ObIODevice *device_handle, blocksstable::ObSSTableMergeRes &merge_res);

public:
  int insert_place_holder_macro_index(const blocksstable::ObLogicMacroBlockId &logic_id);
  int update_logic_id_to_macro_index(const blocksstable::ObLogicMacroBlockId &logic_id, const ObBackupMacroBlockIndex &index);
  int check_place_holder_macro_index_exist(const blocksstable::ObLogicMacroBlockId &logic_id, bool &exist);
  int check_real_macro_index_exist(const blocksstable::ObLogicMacroBlockId &logic_id, bool &exist, ObBackupMacroBlockIndex &index);

private:
  static const int64_t BUCKET_NUM = 10000;

private:
  bool is_inited_;
  lib::ObMutex mutex_;
  common::ObTabletID tablet_id_;
  common::ObArray<storage::ObITable::TableKey> table_keys_;
  common::ObArray<blocksstable::ObSSTableIndexBuilder *> builders_;
  common::ObArray<blocksstable::ObSSTableMergeRes> merge_results_;
  common::hash::ObHashMap<blocksstable::ObLogicMacroBlockId, ObBackupMacroBlockIndex> local_reuse_map_;
  bool is_major_compaction_mview_dep_tablet_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletSSTableIndexBuilderMgr);
};

}
}

#endif
