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

#ifndef OCEANBASE_STORAGE_OB_TABLET_TABLE_STORE_H_
#define OCEANBASE_STORAGE_OB_TABLET_TABLE_STORE_H_

#include "storage/blocksstable/ob_sstable.h"
#include "storage/memtable/ob_memtable.h"
#include "ob_table_store_util.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
}
namespace storage
{

class ObSSTableArray;
class ObMemtableArray;
class ObTableStoreIterator;
class ObIMemtableMgr;
class ObTablet;
struct ObUpdateTableStoreParam;
struct ObBatchUpdateTableStoreParam;

class ObTabletTableStore
{
public:
  friend class ObPrintTableStore;
  typedef common::ObSEArray<ObTableHandleV2, MAX_SSTABLE_CNT_IN_STORAGE> ObTableHandleArray;
  enum ExtendTable: int64_t {
    META_MAJOR = 0,
    EXTEND_CNT
  };

  ObTabletTableStore();
  virtual ~ObTabletTableStore();

  // handle is not null when first creating table store
  int init(
      common::ObIAllocator &allocator,
      ObTablet *tablet,
      ObTableHandleV2 * const handle = nullptr);
  int init(
      common::ObIAllocator &allocator,
      ObTablet *tablet,
      const ObUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      ObTablet *tablet,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;

  void reset();

  OB_INLINE bool is_valid() const;
  OB_INLINE bool is_ready_for_read() const { return is_ready_for_read_; }
  OB_INLINE int64_t get_table_count() const { return major_tables_.count() + minor_tables_.count(); }
  OB_INLINE ObTablet *get_tablet() { return tablet_ptr_; }

  OB_INLINE ObSSTableArray &get_major_sstables() { return major_tables_; }
  OB_INLINE ObSSTableArray &get_minor_sstables() { return minor_tables_; }
  OB_INLINE ObSSTableArray &get_ddl_sstables() { return ddl_sstables_; }
  OB_INLINE ObSSTableArray &get_ddl_memtables() { return ddl_mem_sstables_; }
  OB_INLINE storage::ObITable *get_extend_sstable(const int64_t type) { return extend_tables_[type]; }
  OB_INLINE const storage::ObSSTableArray &get_major_sstables() const { return major_tables_; }
  OB_INLINE const storage::ObSSTableArray &get_minor_sstables() const { return minor_tables_; }
  OB_INLINE const storage::ObSSTableArray &get_ddl_sstables() const { return ddl_sstables_; }
  OB_INLINE const storage::ObSSTableArray &get_ddl_memtables() const { return ddl_mem_sstables_; }
  OB_INLINE storage::ObITable *get_extend_sstable(const int64_t type) const { return extend_tables_[type]; }

  // called by SSTable Garbage Collector Thread
  int need_remove_old_table(
      const int64_t multi_version_start,
      bool &need_remove) const;

  int get_table(const ObITable::TableKey &table_key, ObTableHandleV2 &handle) const;
  int get_read_tables(const int64_t snapshot_version,
                      ObTableStoreIterator &iterator,
                      const bool allow_no_ready_read = false);
  int get_read_major_sstable(const int64_t &major_snapshot_version,
                             ObTableStoreIterator &iterator);

  int get_memtables(common::ObIArray<storage::ObITable *> &memtables, const bool need_active = false) const;
  int64_t get_memtables_count() const
  {
    return memtables_.count();
  }
  int prepare_memtables();
  int update_memtables();
  int clear_memtables();
  int get_first_frozen_memtable(ObITable *&table);

  int get_ddl_sstable_handles(ObTablesHandleArray &ddl_sstable_handles) const;
  int get_mini_minor_sstables(ObTablesHandleArray &minor_sstables) const;
  int assign(common::ObIAllocator &allocator, const ObTabletTableStore &other, ObTablet *new_tablet);
  int get_recycle_version(const int64_t multi_version_start, int64_t &recycle_version) const;

  int64_t to_string(char *buf, const int64_t buf_len) const;

  //ha
  int get_ha_tables(
      ObTableStoreIterator &iterator,
      bool &is_ready_for_read);
  int build_ha_new_table_store(
      common::ObIAllocator &allocator,
      ObTablet *tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int batch_replace_sstables(
      common::ObIAllocator &allocator,
      ObTablet *tablet,
      const ObIArray<ObTableHandleV2> &table_handles,
      const ObTabletTableStore &old_store);

private:
  int build_new_table_store(
      common::ObIAllocator &allocator,
      const ObUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int pull_memtables();
  int init_read_cache();
  bool check_read_cache(const int64_t snapshot_version);

  int calculate_read_tables(
      ObTableStoreIterator &iterator,
      const int64_t snapshot_version,
      const bool allow_no_ready_read = false);
  int calculate_read_memtables(const ObTablet &tablet, ObTableStoreIterator &iterator);

  bool check_read_tables(
       ObTableStoreIterator &iterator,
       const int64_t snapshot_version);

  int build_major_tables(
      common::ObIAllocator &allocator,
      const ObUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store,
      int64_t &inc_base_snapshot_version);
  int build_minor_tables(
      common::ObIAllocator &allocator,
      const ObUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store,
      const int64_t inc_base_snapshot_version);
  int build_meta_major_table(
      const ObTableHandleV2 &new_handle,
      const ObTabletTableStore &old_store);

  int inner_build_major_tables_(
      common::ObIAllocator &allocator,
      const ObTabletTableStore &old_store,
      const ObTablesHandleArray &tables_handle,
      const int64_t multi_version_start,
      const bool allow_duplicate_sstable,
      int64_t &inc_base_snapshot_version);

  int check_ready_for_read();
  int check_continuous() const;
  int build_ha_new_table_store_(
      common::ObIAllocator &allocator,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int build_ha_major_tables_(
      common::ObIAllocator &allocator,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store,
      int64_t &inc_base_snapshot_version);
  int build_ha_minor_tables_(
      common::ObIAllocator &allocator,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store,
      const int64_t inc_base_snapshot_version);
  int build_ha_ddl_tables_(
      common::ObIAllocator &allocator,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int cut_ha_sstable_scn_range_(
      common::ObIArray<ObITable *> &minor_sstables,
      ObTablesHandleArray &tables_handle);
  int check_minor_tables_continue_(
      const int64_t count,
      ObITable **minor_sstables) const;
  int combin_ha_minor_sstables_(
      common::ObIArray<ObITable *> &old_store_minor_sstables,
      common::ObIArray<ObITable *> &need_add_minor_sstables,
      common::ObIArray<ObITable *> &new_minor_sstables);
  int check_old_store_minor_sstables_(
      common::ObIArray<ObITable *> &old_store_minor_sstables);
  int get_ha_mini_minor_sstables_(
      ObTablesHandleArray &minor_sstables) const;
  int replace_ha_minor_sstables_(
      common::ObIAllocator &allocator,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store,
      const int64_t inc_base_snapshot_version);
  int update_ha_minor_sstables_(
      common::ObIAllocator &allocator,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  // ddl
  int pull_ddl_memtables(common::ObIAllocator &allocator);
  int build_ddl_sstables(
      common::ObIAllocator &allocator,
      const ObUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  bool is_major_sstable_empty() const { return major_tables_.empty() && ddl_mem_sstables_.empty() && ddl_sstables_.empty(); }
  int get_ddl_major_sstables(ObIArray<ObITable *> &ddl_major_sstables);

  int inner_replace_sstables(
      common::ObIAllocator &allocator,
      const ObIArray<ObTableHandleV2> &table_handles,
      const ObTabletTableStore &old_store);
  int get_replaced_tables(
      const ObIArray<ObTableHandleV2> &table_handles,
      const ObITableArray &old_tables,
      ObSEArray<ObITable *, 8> &replaced_tables) const;
  int deep_copy_sstable_(const blocksstable::ObSSTable &old_sstable,
      ObTableHandleV2 &new_table_handle);

public:
  static const int64_t TABLE_STORE_VERSION = 0x0100;
  static const int64_t MAX_SSTABLE_CNT = 128;
private:
  ObTablet *tablet_ptr_;
  ObSSTableArray major_tables_;
  ObSSTableArray minor_tables_;
  ObSSTableArray ddl_sstables_;
  storage::ObExtendTableArray extend_tables_;
  storage::ObMemtableArray memtables_;
  ObSSTableArray ddl_mem_sstables_;
  storage::ObTableStoreIterator read_cache_;
  int64_t version_;
  bool is_ready_for_read_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletTableStore);
};

/* attention! It maybe unsafe to use this class without holding ObTablet::table_store_lock_ */
class ObPrintTableStore
{
public:
  ObPrintTableStore(const ObTabletTableStore &table_store);
  virtual ~ObPrintTableStore();
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  void table_to_string(
       ObITable *table,
       const char* table_arr,
       char *buf,
       const int64_t buf_len,
       int64_t &pos) const;

  void print_arr(
      const ObITableArray &tables,
      const char* table_arr,
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      bool &is_print) const;

  void print_mem(
      const ObMemtableArray &tables,
      const char* table_arr,
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      bool &is_print) const;

private:
  const ObSSTableArray &major_tables_;
  const ObSSTableArray &minor_tables_;
  const ObMemtableArray &memtables_;
  const ObSSTableArray &ddl_mem_sstables_;
  const ObSSTableArray &ddl_sstables_;
  const ObExtendTableArray &extend_tables_;
  const bool is_ready_for_read_;
};

OB_INLINE bool ObTabletTableStore::is_valid() const
{
  return is_inited_ &&
         get_table_count() >= 0 &&
         get_table_count() <= ObTabletTableStore::MAX_SSTABLE_CNT;
}

}//storage
}//oceanbase


#endif /* OCEANBASE_STORAGE_OB_TABLET_TABLE_STORE_H_ */
