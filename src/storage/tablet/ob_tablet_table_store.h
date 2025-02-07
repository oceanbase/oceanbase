/**
 * Copyright (c) 2022 OceanBase
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
#include "storage/tablet/ob_table_store_util.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/blocksstable/ob_major_checksum_info.h"
namespace oceanbase
{
namespace storage
{
class ObIMemtableMgr;
struct ObUpdateTableStoreParam;
struct UpdateUpperTransParam;
struct ObBatchUpdateTableStoreParam;
class ObTablet;
class ObTableStoreIterator;
class ObCachedTableHandle;
class ObStorageMetaHandle;
struct ObTabletHAStatus;

enum class ObGetReadTablesMode : uint8_t
{
  NORMAL = 0,
  ALLOW_NO_READY_READ = 1,
  SKIP_MAJOR = 2
};

class ObReadyForReadParam final
{
// if you want to add a member variable, please add here.
#define LIST_MEMBERS                               \
    DO_SOMETHING(share::SCN, ddl_commit_scn_)      \
    DO_SOMETHING(share::SCN, clog_checkpoint_scn_)
public:
  ObReadyForReadParam() = default;
  ~ObReadyForReadParam() = default;
  int assign(const ObReadyForReadParam &other)
  {
    int ret = OB_SUCCESS;
#define DO_SOMETHING(type, name) name = other.name;
    LIST_MEMBERS
#undef DO_SOMETHING
    return ret;
  }
  bool operator==(const ObReadyForReadParam &other) const
  {
    bool is_equal = false;
    // check for equality;
#define DO_SOMETHING(type, name) is_equal = is_equal && (name == other.name);
    LIST_MEMBERS
#undef DO_SOMETHING
    return is_equal;
  }
  bool operator!=(const ObReadyForReadParam &other) const { return !(*this == other); }
public:
// define member variables
#define DO_SOMETHING(type, name) type name;
    LIST_MEMBERS
#undef DO_SOMETHING
private:
  DISALLOW_COPY_AND_ASSIGN(ObReadyForReadParam);
};

class ObTabletTableStore : public ObIStorageMetaObj
{
public:
  friend class ObPrintTableStore;
  ObTabletTableStore();
  virtual ~ObTabletTableStore();

  void reset();
  // first init
  int init(
      ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const blocksstable::ObSSTable *sstable = nullptr,
      // when first create tablet in migration, will carry ObMajorChecksumInfo from src svr
      const ObMajorChecksumInfo *ckm_info = nullptr);
  // init for update
  int init(
      ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
#ifdef OB_BUILD_SHARED_STORAGE
  // init for shared storage major compaction
  int init_for_shared_storage(
      common::ObArenaAllocator &allocator,
      const ObUpdateTableStoreParam &param);
#endif
  // init temp table store with address for serialize, no memtable array
  // need get all member variables from old table store
  int init(
      ObArenaAllocator &allocator,
      common::ObIArray<ObITable *> &sstable_array,
      common::ObIArray<ObMetaDiskAddr> &addr_array,
      const blocksstable::ObMajorChecksumInfo &major_ckm_info);
  // init for replace sstables in table store
  int init(
      ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObIArray<ObITable *> &replace_sstable_array,
      const ObTabletTableStore &old_store);
  // init from old table store directly
  int init(
      ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObTabletTableStore &old_store);
  virtual int64_t get_deep_copy_size() const override;
  virtual int64_t get_try_cache_size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIStorageMetaObj *&value) const override;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;
  OB_INLINE bool is_valid() const
  {
    return is_inited_ &&
        get_table_count() >= 0 &&
        get_table_count() <= ObTabletTableStore::MAX_SSTABLE_CNT;
  }
  OB_INLINE bool is_ready_for_read() const { return is_ready_for_read_; }
  OB_INLINE int64_t get_table_count() const { return major_tables_.count() + minor_tables_.count(); }
  OB_INLINE int64_t get_ddl_memtable_count() const { return ddl_mem_sstables_.count(); }

  // Interfaces below that access sstable array member of table store directly does not guarantee
  // sstables in array were loaded
  OB_INLINE const storage::ObSSTableArray &get_major_sstables() const { return major_tables_; }
  OB_INLINE const storage::ObSSTableArray &get_minor_sstables() const { return minor_tables_; }
  OB_INLINE const storage::ObSSTableArray &get_ddl_sstables() const { return ddl_sstables_; }
  OB_INLINE const storage::ObSSTableArray &get_mds_sstables() const { return mds_sstables_; }
  OB_INLINE const storage::ObSSTableArray &get_meta_major_sstables() const { return meta_major_tables_; }
  OB_INLINE blocksstable::ObSSTable *get_meta_major_sstable() const
  {
    return meta_major_tables_.empty() ? nullptr : meta_major_tables_.at(0);
  }
  OB_INLINE bool is_table_valid_mds_or_minor_sstable(const ObITable &table, const bool is_mds)
  {
    return (is_mds && table.is_mds_sstable()) || (!is_mds && table.is_minor_sstable());
  }

  int inc_macro_ref() const;
  void dec_macro_ref() const;
  // called by SSTable Garbage Collector Thread
  int need_remove_old_table(
      const int64_t multi_version_start,
      bool &need_remove) const;

  // won't set secondary meta handle in CachedTableHandle for memtable
  // FIXME: make lifetime of such interface safer?
  int get_table(
      const ObStorageMetaHandle &table_store_handle,
      const ObITable::TableKey &table_key,
      ObTableHandleV2 &handle) const;
  int get_sstable(const ObITable::TableKey &table_key, ObSSTableWrapper &wrapper) const;
  int get_memtable(const ObITable::TableKey &table_key, ObITable *&table) const;
  int get_read_tables(
      const int64_t snapshot_version,
      const ObTablet &tablet,
      ObTableStoreIterator &iter,
      const ObGetReadTablesMode mode = ObGetReadTablesMode::NORMAL) const;
  int get_all_sstable(ObTableStoreIterator &iter, const bool unpack_co_table = false) const;
  int get_read_major_sstable(const int64_t snapshot_version, ObTableStoreIterator &iter) const;
  int get_memtables(common::ObIArray<storage::ObITable *> &memtables) const;
  int update_memtables(const common::ObIArray<storage::ObITable *> &memtables);
  int clear_memtables();
  int get_first_frozen_memtable(ObITable *&table) const;
  int get_ddl_sstables(ObTableStoreIterator &iter) const;
  int get_mds_sstables(ObTableStoreIterator &iter) const;
  int get_mini_minor_sstables(
      ObTableStoreIterator &iter) const;
  int get_recycle_version(const int64_t multi_version_start, int64_t &recycle_version) const;
  int get_ha_tables(ObTableStoreIterator &iter, bool &is_ready_for_read) const;
  int build_ha_new_table_store(
      common::ObArenaAllocator &allocator,
      ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  // Asynchronously batch cache sstable meta according to the specified memory size.
  //  - Here remain_size doesn't include the size of the table store itself.
  //  - Taking the bypass don't pollute the meta cache.
  //  - Asynchronously batch performance is better.
  int batch_cache_sstable_meta(
      common::ObArenaAllocator &allocator,
      const int64_t remain_size);
  int cache_sstable_meta(
      common::ObArenaAllocator &allocator,
      blocksstable::ObSSTableMetaHandle &meta_handle,
      blocksstable::ObSSTable *sstable,
      int64_t &remain_size);
  int check_ready_for_read(const ObReadyForReadParam &param);
  static void diagnose_table_count_unsafe(const ObTablet &tablet);
  int64_t to_string(char *buf, const int64_t buf_len) const;

  // Load sstable with @addr, loaded object lifetime guaranteed by @handle
  static int load_sstable(
      const ObMetaDiskAddr &addr,
      const bool load_co_sstable,
      ObStorageMetaHandle &handle);
  // load @orig_sstable on demand, return @loaded_sstable.
  // Lifetime guaranteed by loaded_sstable_handle if is loaded.
  static int load_sstable_on_demand(
      const ObStorageMetaHandle &table_store_handle,
      blocksstable::ObSSTable &orig_sstable,
      ObStorageMetaHandle &loaded_sstable_handle,
      blocksstable::ObSSTable *&loaded_sstable);
  // ddl-split
  int build_split_new_table_store(
      common::ObArenaAllocator &allocator,
      ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  const blocksstable::ObMajorChecksumInfo &get_major_ckm_info() const { return major_ckm_info_; }
  int get_all_minor_sstables(ObTableStoreIterator &iter) const;
private:
  int get_need_to_cache_sstables(
      common::ObIArray<ObStorageMetaValue::MetaType> &meta_types,
      common::ObIArray<ObStorageMetaKey> &keys,
      common::ObIArray<blocksstable::ObSSTable *> &sstables);
  int get_need_to_cache_sstables(
      const ObSSTableArray &sstable_array,
      common::ObIArray<ObStorageMetaValue::MetaType> &meta_types,
      common::ObIArray<ObStorageMetaKey> &keys,
      common::ObIArray<blocksstable::ObSSTable *> &sstables);
  int batch_cache_sstable_meta(
      common::ObArenaAllocator &allocator,
      const int64_t limit_size,
      common::ObIArray<blocksstable::ObSSTable *> &sstables,
      common::ObIArray<ObStorageMetaHandle> &handles);
  int get_local_sstable_size_limit(
      const int64_t table_store_mem_ctx_size,
      int64_t &local_sstable_size_limit) const;
  int build_new_table_store(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int build_memtable_array(const ObTablet &tablet);
  // Synchronous cache sstable meta with 2MB memory.
  //  - The cache size is 2M, and include the size of the table store itself.
  //  - Synchronous cache sstable meta, and one by one.
  int try_cache_local_sstables(common::ObArenaAllocator &allocator);
  int calculate_read_tables(
      const int64_t snapshot_version,
      const ObTablet &tablet,
      ObTableStoreIterator &iterator,
      const bool allow_no_ready_read,
      const bool skip_major) const;
  int calculate_read_memtables(const ObTablet &tablet, ObTableStoreIterator &iterator) const;
  bool check_read_tables(
      const ObTablet &tablet,
      const int64_t snapshot_version,
      const blocksstable::ObSSTable *base_table) const;
  int build_major_tables(
      common::ObArenaAllocator &allocator,
      const ObUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store,
      int64_t &inc_base_snapshot_version);
  int build_minor_tables(
      common::ObArenaAllocator &allocator,
      const blocksstable::ObSSTable *new_sstable,
      const ObTabletTableStore &old_store,
      const bool need_check_sstable,
      const int64_t inc_base_snapshot_version,
      const ObTabletHAStatus &ha_status,
      const UpdateUpperTransParam &upper_trans_param,
      const bool is_mds);
  int inner_process_minor_tables(
      common::ObArenaAllocator &allocator,
      const ObTabletTableStore &old_store,
      ObArray<ObITable *> &minor_tables,
      const int64_t inc_base_snapshot_version,
      const ObTabletHAStatus &ha_status,
      const UpdateUpperTransParam &upper_trans_param,
      const bool is_mds);
  int build_meta_major_table(
      common::ObArenaAllocator &allocator,
      const blocksstable::ObSSTable *new_sstable,
      const ObTabletTableStore &old_store);
  int inner_build_major_tables_(
      common::ObArenaAllocator &allocator,
      const ObTabletTableStore &old_store,
      const ObIArray<ObITable *> &tables_array,
      const int64_t multi_version_start,
      const bool allow_duplicate_sstable,
      int64_t &inc_base_snapshot_version,
      bool replace_old_row_store_major = false);
  int check_and_build_new_major_tables(
      const ObTabletTableStore &old_store,
      const ObIArray<ObITable *> &tables_array,
      const bool allow_duplicate_sstable,
      ObIArray<ObITable *> &major_tables) const;
  int inner_replace_remote_major_sstable_(
      common::ObArenaAllocator &allocator,
      const ObTabletTableStore &old_store,
      ObITable *new_table);

  int check_ready_for_read(const ObTablet &tablet);
  int check_continuous() const;
  template <class T>
  int check_minor_tables_continue_(T &minor_tables) const;
  int cache_local_sstable_meta(
      ObArenaAllocator &allocator,
      blocksstable::ObSSTable *array_sstable,
      const blocksstable::ObSSTable *loaded_sstable,
      const int64_t local_sstable_size_limit,
      int64_t &local_sstable_meta_size);
  int try_cache_local_sstable_meta(
      ObArenaAllocator &allocator,
      ObSSTableArray &sstable_array,
      const int64_t local_sstable_size_limit,
      int64_t &local_sstable_meta_size);
  // ha
  int check_new_sstable_can_be_accepted_(
      common::ObIArray<ObITable *> &old_tables,
      ObITable *new_table);
  int check_new_major_sstable_can_be_accepted_(
      const ObTabletTableStore &old_store,
      ObITable *major_table);
  int build_ha_new_table_store_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int build_ha_major_tables_(
      common::ObArenaAllocator &allocator,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store,
      const int64_t &multi_version_start,
      int64_t &inc_base_snapshot_version);

  int build_ha_minor_tables_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store,
      const int64_t inc_base_snapshot_version);
  int build_ha_ddl_tables_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int replace_ha_ddl_tables_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int replace_ha_remote_ddl_tables_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int build_ha_mds_tables_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int replace_ha_mds_tables_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int replace_ha_remote_mds_tables_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int cut_ha_sstable_scn_range_(
      common::ObArenaAllocator &allocator,
      common::ObIArray<ObITable *> &orig_minor_sstables,
      common::ObIArray<ObITable *> &cut_minor_sstables);
  int check_minor_tables_continue_(
      const int64_t count,
      ObITable **minor_sstables) const;
  int check_minor_table_continue_(
      ObITable *table,
      ObITable *&prev_table) const;
  int combine_ha_multi_version_sstables_(
      const share::SCN &scn,
      common::ObIArray<ObITable *> &old_store_sstables,
      common::ObIArray<ObITable *> &need_add_sstables,
      common::ObIArray<ObITable *> &new_sstables);
  int combine_transfer_minor_sstables_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      common::ObIArray<ObITable *> &old_store_minor_sstables,
      common::ObIArray<ObITable *> &need_add_minor_sstables,
      const ObBatchUpdateTableStoreParam &param,
      common::ObIArray<ObITable *> &new_minor_sstables);
  int check_old_store_minor_sstables_(
      common::ObIArray<ObITable *> &old_store_minor_sstables);
  int replace_ha_minor_sstables_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store,
      const int64_t inc_base_snapshot_version);
  int replace_ha_remote_minor_tables_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store,
      const int64_t inc_base_snapshot_version);
  int replace_transfer_minor_sstables_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);

  // ddl
  int pull_ddl_memtables(common::ObArenaAllocator &allocator, const ObTablet &tablet);
  int build_ddl_sstables(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const blocksstable::ObSSTable *new_sstable,
      const bool keep_old_ddl_sstable,
      const ObTabletTableStore &old_store);
  bool is_major_sstable_empty(const share::SCN &ddl_commit_scn) const;
  int get_ddl_major_sstables(ObIArray<ObITable *> &ddl_major_sstables) const;
  // ddl-split
  int check_skip_split_tables_exist_(
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store,
      const ObTablet &tablet);
  int build_split_new_table_store_(
      common::ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      const ObTabletTableStore &old_store);
  int build_split_minor_tables_(
      common::ObArenaAllocator &allocator,
      const ObTabletTableStore &old_store,
      const ObIArray<ObITable *> &tables_array,
      const int64_t inc_base_snapshot_version,
      const ObTabletHAStatus &ha_status);

  int inner_replace_sstables(
      common::ObArenaAllocator &allocator,
      const ObIArray<ObITable *> &table_handles,
      const ObTabletTableStore &old_store);
  int replace_sstables(
      common::ObArenaAllocator &allocator,
      const ObIArray<ObITable *> &replace_sstable_array,
      const ObSSTableArray &old_tables,
      ObSSTableArray &new_tables) const;
  int build_major_checksum_info(
    const ObTabletTableStore &old_store,
    const ObUpdateTableStoreParam *param,
    ObArenaAllocator &allocator);
  int replace_ha_remote_sstables_(
      const common::ObIArray<ObITable *> &old_store_sstables,
      const ObTablesHandleArray &new_tables_handle,
      const bool check_continue,
      common::ObIArray<ObITable *> &out_sstables);
  int get_mini_minor_sstables_(ObTableStoreIterator &iter) const;
  template<typename ...Args>
  int init_minor_sstable_array_with_check(ObSSTableArray &minor_sstable_array, Args&& ...args);

public:
  static const int64_t TABLE_STORE_VERSION_V1 = 0x0100;
  static const int64_t TABLE_STORE_VERSION_V2 = 0x0101;
  static const int64_t TABLE_STORE_VERSION_V3 = 0x0102;
  static const int64_t TABLE_STORE_VERSION_V4 = 0x0103; // for major_ckm_info_
  static const int64_t MAX_SSTABLE_CNT = 128;
  // limit table store memory size to one ACHUNK
  static const int64_t MAX_TABLE_STORE_MEMORY_SIZE= lib::ACHUNK_SIZE - lib::AOBJECT_META_SIZE;
  static const int64_t EMERGENCY_SSTABLE_CNT = 48;
private:
  int64_t version_;
  ObSSTableArray major_tables_;
  ObSSTableArray minor_tables_;
  ObSSTableArray ddl_sstables_;
  ObSSTableArray meta_major_tables_;
  ObSSTableArray mds_sstables_;
  ObMemtableArray memtables_;
  ObDDLKVArray ddl_mem_sstables_;
  blocksstable::ObMajorChecksumInfo major_ckm_info_;
  // Attention! if add new member variables, need fix serialize/deserialize & all init func
  mutable common::SpinRWLock memtables_lock_; // protect memtable read and update after inited
  bool is_ready_for_read_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletTableStore);
};

/* attention! It maybe unsafe to use this class without holding ObTablet::table_store_lock_ */
class ObPrintTableStore
{
public:
  ObPrintTableStore(const ObTabletTableStore &table_store);
  virtual ~ObPrintTableStore() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  void table_to_string(
       ObITable *table,
       const char* table_arr,
       char *buf,
       const int64_t buf_len,
       int64_t &pos) const;

  void ddl_kv_to_string(
       ObDDLKV *table,
       const char* table_arr,
       char *buf,
       const int64_t buf_len,
       int64_t &pos) const;

  void print_arr(
      const ObSSTableArray &tables,
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

  void print_ddl_mem(
      const ObDDLKVArray &tables,
      const char* table_arr,
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      bool &is_print) const;

private:
  const ObSSTableArray &major_tables_;
  const ObSSTableArray &minor_tables_;
  const ObMemtableArray &memtables_;
  const ObDDLKVArray &ddl_mem_sstables_;
  const ObSSTableArray &ddl_sstables_;
  const ObSSTableArray &meta_major_tables_;
  const ObSSTableArray &mds_sstables_;
  const bool is_ready_for_read_;
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_TABLET_TABLE_STORE_H_ */
