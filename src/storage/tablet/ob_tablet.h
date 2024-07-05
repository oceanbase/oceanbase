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

#ifndef OCEANBASE_STORAGE_TABLET_OB_TABLET
#define OCEANBASE_STORAGE_TABLET_OB_TABLET

#include "lib/atomic/ob_atomic.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "storage/ob_storage_schema.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_storage_table_guard.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tablet_pointer_handle.h"
#include "storage/tablet/ob_tablet_complex_addr.h"
#include "storage/tablet/ob_tablet_member_wrapper.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_tablet_table_store_flag.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_mds_data_cache.h"
#include "storage/tablet/ob_tablet_block_aggregated_info.h"
#include "storage/tablet/ob_tablet_block_header.h"
#include "storage/tablet/ob_tablet_space_usage.h"
#include "storage/tx/ob_trans_define.h"
#include "share/scn.h"
#include "ob_i_tablet_mds_interface.h"
#include <type_traits>

namespace oceanbase
{
namespace share
{
class ObLSID;
struct ObTabletAutoincInterval;
}

namespace logservice
{
class ObLogHandler;
}

namespace transaction
{
class ObTransID;
}

namespace logservice
{
class ObTabletReplayExecutor;
}

namespace observer
{
class ObAllVirtualMdsNodeStat;
}

namespace storage
{
class ObIMemtable;
class ObStoreCtx;
class ObTableHandleV2;
class ObFreezer;
class ObTabletDDLInfo;
class ObTabletDDLKvMgr;
class ObDDLKVHandle;
class ObTabletTableIterator;
class ObMetaDiskAddr;
class ObUpdateTabletPointerParam;
class ObTabletCreateDeleteMdsUserData;
class ObTabletBindingMdsUserData;
class ObMemtableArray;
class ObCOSSTableV2;
class ObMacroInfoIterator;
class ObMdsRowIterator;
class ObMdsMiniMergeOperator;

struct ObTableStoreCache
{
public:
  ObTableStoreCache();
  ~ObTableStoreCache() { reset(); }
  void reset();
  int init(
      const ObSSTableArray &major_tables,
      const ObSSTableArray &minor_tables,
      const bool is_row_store);
  void assign(const ObTableStoreCache &other);
  TO_STRING_KV(K_(last_major_snapshot_version), K_(major_table_cnt),
      K_(minor_table_cnt), K_(recycle_version), K_(last_major_column_count), K_(is_row_store),
      K_(last_major_compressor_type), K_(last_major_latest_row_store_type));

public:
  int64_t last_major_snapshot_version_;
  int64_t major_table_cnt_;
  int64_t minor_table_cnt_;
  int64_t recycle_version_;
  int64_t last_major_column_count_;
  bool is_row_store_;
  common::ObCompressorType last_major_compressor_type_;
  common::ObRowStoreType last_major_latest_row_store_type_;
};

class ObTablet final : public ObITabletMdsInterface
{
  friend class ObLSTabletService;
  friend class ObTabletPointer;
  friend class ObTabletMediumInfoReader;
  friend class logservice::ObTabletReplayExecutor;
  friend class ObTabletPersister;
  friend class ObTabletPointerMap;
  friend class observer::ObAllVirtualMdsNodeStat;// for virtual table to show inner mds states
  friend class ObTabletTableIterator;
public:
  typedef ObMetaObjGuard<ObTabletDDLKvMgr> ObDDLKvMgrHandle;
  typedef common::ObSEArray<ObTableHandleV2, BASIC_MEMSTORE_CNT> ObTableHandleArray;
  typedef common::ObFixedArray<share::schema::ObColDesc, common::ObIAllocator> ColDescArray;
public:
  ObTablet();
  ObTablet(const ObTablet&) = delete;
  ObTablet &operator=(const ObTablet&) = delete;
  virtual ~ObTablet();
public:
  void reset();
  bool is_ls_inner_tablet() const;
  bool is_ls_tx_data_tablet() const;
  bool is_ls_tx_ctx_tablet() const;
  void update_wash_score(const int64_t score);
  void inc_ref();
  int64_t dec_ref();
  int64_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }
  int64_t get_wash_score() const { return ATOMIC_LOAD(&wash_score_); }
  int get_rec_log_scn(share::SCN &rec_scn);
  int get_max_sync_medium_scn(int64_t &max_medium_scn) const;
  int get_max_sync_storage_schema_version(int64_t &max_schema_version) const;
  inline int64_t get_last_major_snapshot_version() const { return table_store_cache_.last_major_snapshot_version_; }
  inline int64_t get_major_table_count() const { return table_store_cache_.major_table_cnt_; }
  inline int64_t get_minor_table_count() const { return table_store_cache_.minor_table_cnt_; }
  inline int64_t get_recycle_version() const { return table_store_cache_.recycle_version_; }
  inline int64_t get_last_major_column_count() const { return table_store_cache_.last_major_column_count_; }
  inline common::ObCompressorType get_last_major_compressor_type() const { return table_store_cache_.last_major_compressor_type_; }
  inline common::ObRowStoreType get_last_major_latest_row_store_type() const { return table_store_cache_.last_major_latest_row_store_type_; }
  inline share::ObLSID get_ls_id() const { return tablet_meta_.ls_id_; }
  inline common::ObTabletID get_tablet_id() const { return tablet_meta_.tablet_id_; }
  inline common::ObTabletID get_data_tablet_id() const { return tablet_meta_.data_tablet_id_; }
  inline int64_t get_last_compaction_scn() const { return tablet_meta_.extra_medium_info_.last_medium_scn_; }
  inline bool is_row_store() const { return table_store_cache_.is_row_store_; }
  int get_mds_table_rec_scn(share::SCN &rec_scn);
  int mds_table_flush(const share::SCN &decided_scn);
  int scan_mds_table_with_op(
      const int64_t mds_construct_sequence,
      ObMdsMiniMergeOperator &op) const;

public:
  // first time create tablet
  int init_for_first_time_creation(
      common::ObArenaAllocator &allocator,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const common::ObTabletID &data_tablet_id,
      const share::SCN &create_scn,
      const int64_t snapshot_version,
      const ObCreateTabletSchema &storage_schema,
      const bool need_create_empty_major_sstable,
      ObFreezer *freezer);
  // dump/merge build new multi version tablet
  int init_for_merge(
      common::ObArenaAllocator &allocator,
      const ObUpdateTableStoreParam &param,
      const ObTablet &old_tablet);
  // transfer build new tablet
  int init_with_migrate_param(
      common::ObArenaAllocator &allocator,
      const ObMigrationTabletParam &param,
      const bool is_update,
      ObFreezer *freezer,
      const bool is_transfer);
  //batch update table store with range cut
  int init_for_sstable_replace(
      common::ObArenaAllocator &allocator,
      const ObBatchUpdateTableStoreParam &param,
      const ObTablet &old_tablet);
  // update medium compaction info mgr and build new tablet
  int init_with_update_medium_info(
      common::ObArenaAllocator &allocator,
      const ObTablet &old_tablet);

  // TODO(@bowen.gbw && @fengjingkun.fjk) tmp interface for force_freeze on column store, should removed later.
  int init_with_new_snapshot_version(
      common::ObArenaAllocator &allocator,
      const ObTablet &old_tablet,
      const int64_t snapshot_version);
  // init for mds table mini merge
  int init_with_mds_sstable(
      common::ObArenaAllocator &allocator,
      const ObTablet &old_tablet,
      const share::SCN &flush_scn,
      const ObUpdateTableStoreParam &param);

  // init for compat
  int init_for_compat(
      common::ObArenaAllocator &allocator,
      const ObTablet &old_tablet,
      ObTableHandleV2 &mds_mini_sstable);

  // batch replace sstables without data modification
  int init_for_defragment(
      common::ObArenaAllocator &allocator,
      const ObIArray<storage::ObITable *> &tables,
      const ObTablet &old_tablet);

  // init empty tablet for delete
  int init_empty_shell(
      ObArenaAllocator &allocator,
      const ObTablet &old_tablet);

  bool is_valid() const;
  // refresh memtable and update tablet_addr_ and table_store_addr_ sequence, only used by slog ckpt
  int refresh_memtable_and_update_seq(const uint64_t seq);
  int32_t get_version() const { return version_; }
  void dec_macro_ref_cnt();
  int inc_macro_ref_cnt();
  // these interfaces is only for tiny mode
  // load_$member: member will always exist in disk(slog file/macro block), so read from disk then deserialize
  // fetch_$member: member may exist in memory or disk, if in memory, get it directly, if in disk,
  //                read from disk then put into kv cache, and return kv cache handle for caller
  int fetch_table_store(ObTabletMemberWrapper<ObTabletTableStore> &wrapper) const;
  int load_macro_info(common::ObArenaAllocator &allocator, ObTabletMacroInfo *&tablet_macro_info, bool &in_memory) const;
  int load_storage_schema(
      common::ObIAllocator &allocator,
      ObStorageSchema *&storage_schema) const;
  int read_medium_info_list(
      common::ObArenaAllocator &allocator,
      const compaction::ObMediumCompactionInfoList *&medium_info_list) const;

  void set_tablet_addr(const ObMetaDiskAddr &tablet_addr);
  void set_allocator(ObArenaAllocator *allocator) { allocator_ = allocator; }
  void set_next_tablet(ObTablet* tablet) { next_tablet_ = tablet; }
  ObTablet *get_next_tablet() { return next_tablet_; }
  ObArenaAllocator *get_allocator() const { return allocator_; }
  bool is_empty_shell() const;
  // major merge or medium merge call
  bool is_data_complete() const;
  int get_ready_for_read_param(ObReadyForReadParam &parm) const;

  // serialize & deserialize
  // TODO: change the impl of serialize and get_serialize_size after rebase
  int serialize(
      char *buf,
      const int64_t len,
      int64_t &pos,
      const ObSArray<ObInlineSecondaryMeta> &meta_arr = ObSArray<ObInlineSecondaryMeta>()) const;
  int deserialize_for_replay(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t len,
    int64_t &pos);

  // for normal tablet deserialize
  int load_deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos);
  int deserialize_post_work(
      common::ObArenaAllocator &allocator);
  int deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos);
  // for 4k tablet
  int deserialize(
      const char *buf,
      const int64_t len,
      int64_t &pos);
  int release_ref_cnt(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos);
  int inc_snapshot_ref_cnt(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos);
  int64_t get_serialize_size(const ObSArray<ObInlineSecondaryMeta> &meta_arr = ObSArray<ObInlineSecondaryMeta>()) const;
  ObMetaObjGuard<ObTablet> &get_next_tablet_guard() { return next_tablet_guard_; }
  const ObMetaObjGuard<ObTablet> &get_next_tablet_guard() const { return next_tablet_guard_; }
  void set_next_tablet_guard(const ObTabletHandle &next_tablet_guard);
  void trim_tablet_list();

  // dml operation
  int insert_rows(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      ObStoreRow *rows,
      ObRowsInfo &rows_info,
      const bool check_exist,
      const ObColDescIArray &col_descs,
      const int64_t row_count,
      const common::ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr);
  int insert_row_without_rowkey_check(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const bool check_exist,
      const ObColDescIArray &col_descs,
      const storage::ObStoreRow &row,
      const common::ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr);
  int update_row(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const ObColDescIArray &col_descs,
      const ObIArray<int64_t> &update_idx,
      const storage::ObStoreRow &old_row,
      const storage::ObStoreRow &new_row,
      const common::ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr);
  int lock_row(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const common::ObNewRow &row);
  int lock_row(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const blocksstable::ObDatumRowkey &rowkey);
  int check_row_locked_by_myself(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const blocksstable::ObDatumRowkey &rowkey,
      bool &locked);
  int get_tablet_first_second_level_meta_ids(ObIArray<blocksstable::MacroBlockId> &meta_ids) const;
  // table operation
  /* When need_unpack is true, if tablet is column store type, we should flatten the co sstable, and add all cg tables to iter.
     Else, we should add co sstable to iter as a whole.
   */
  int get_all_tables(ObTableStoreIterator &iter, const bool need_unpack = false) const;
  int get_all_sstables(ObTableStoreIterator &iter, const bool need_unpack = false) const;
  int get_tablet_size(const bool ignore_shared_block, int64_t &meta_size, int64_t &data_size);
  int get_memtables(common::ObIArray<storage::ObITable *> &memtables, const bool need_active = false) const;
  int get_ddl_kvs(common::ObIArray<ObDDLKV *> &ddl_kvs) const;

  // memtable operation
  // ATTENTION!!!
  // - The `get_all_memtables()` is that get all memtables from memtable mgr, not from this tablet.
  int get_all_memtables(ObTableHdlArray &handle) const;
  int get_boundary_memtable_from_memtable_mgr(ObTableHandleV2 &handle) const;
  int get_protected_memtable_mgr_handle(ObProtectedMemtableMgrHandle *&handle) const;

  // get the active memtable for write or replay.
  int get_active_memtable(ObTableHandleV2 &handle) const;

  // ATTENTION!!!
  // 1. release memtables from memtable manager and this tablet.
  // 2. If a tablet may be being accessed, shouldn't call this function.
  int rebuild_memtables(const share::SCN scn);

  void reset_memtable();
  // ATTENTION!!! The following two interfaces only release memtable from memtable manager.
  int release_memtables(const share::SCN scn);
  // force release all memtables
  // just for rebuild or migrate retry.
  int release_memtables();

  int wait_release_memtables();

  int get_storage_schema_for_transfer_in(
      common::ObArenaAllocator &allocator,
      ObStorageSchema &storage_schema) const;

  int get_restore_status(ObTabletRestoreStatus::STATUS &restore_status);

  // static help function
  static int deserialize_id(
      const char *buf,
      const int64_t len,
      share::ObLSID &ls_id,
      common::ObTabletID &tablet_id);
  static int check_transfer_seq_equal(const ObTablet &tablet, const int64_t transfer_seq);
  int rowkey_exists(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      const common::ObNewRow &row,
      bool &exists);
  int rowkeys_exists(
      ObStoreCtx &store_ctx,
      ObRelativeTable &relative_table,
      ObRowsInfo &rows_info,
      bool &exists);

  // migration section
  // used for migration source generating create tablet rpc argument
  int build_migration_tablet_param(
      ObMigrationTabletParam &mig_tablet_param) const;
  int build_migration_sstable_param(
      const ObITable::TableKey &table_key,
      blocksstable::ObMigrationSSTableParam &mig_sstable_param) const;
  int get_ha_tables(
      ObTableStoreIterator &iter,
      bool &is_ready_for_read);
  int get_ha_sstable_size(int64_t &data_size);
  //transfer
  int build_transfer_tablet_param(
      const int64_t data_version,
      const share::ObLSID &dest_ls_id,
      ObMigrationTabletParam &mig_tablet_param);
  int build_transfer_backfill_tablet_param(
      const ObTabletMeta &src_tablet_meta,
      const ObStorageSchema &src_storage_schema,
      ObMigrationTabletParam &param) const;

  int get_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle, bool try_create = false);
  int set_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int remove_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int start_direct_load_task_if_need();
  int get_ddl_sstables(ObTableStoreIterator &table_store_iter) const;
  int get_mds_sstables(ObTableStoreIterator &table_store_iter) const;
  int get_mini_minor_sstables(ObTableStoreIterator &table_store_iter) const;
  int get_table(const ObITable::TableKey &table_key, ObTableHandleV2 &handle) const;
  int get_recycle_version(const int64_t multi_version_start, int64_t &recycle_version) const;
  int get_migration_sstable_size(int64_t &data_size);

  // other
  const ObMetaDiskAddr &get_tablet_addr() const { return tablet_addr_; }
  const ObTabletMeta &get_tablet_meta() const { return tablet_meta_; }
  share::SCN get_clog_checkpoint_scn() const { return tablet_meta_.clog_checkpoint_scn_; }
  share::SCN get_mds_checkpoint_scn() const { return tablet_meta_.mds_checkpoint_scn_; }
  int64_t get_snapshot_version() const { return tablet_meta_.snapshot_version_; }
  int64_t get_multi_version_start() const { return tablet_meta_.multi_version_start_; }
  int get_multi_version_start(share::SCN &scn) const;
  int get_snapshot_version(share::SCN &scn) const;

  //TODO huronghui.hrh: rename function for row store sstable
  const ObITableReadInfo &get_rowkey_read_info() const { return *rowkey_read_info_; }
  const ObTabletPointerHandle &get_pointer_handle() { return pointer_hdl_; }

  int get_meta_disk_addr(ObMetaDiskAddr &addr) const;

  int assign_pointer_handle(const ObTabletPointerHandle &ptr_hdl);

  int replay_update_storage_schema(
      const share::SCN &scn,
      const char *buf,
      const int64_t buf_size,
      int64_t &pos);
  //Deprecated interface, DONOT use it anymore
  int get_schema_version_from_storage_schema(int64_t &schema_version) const;
  // get MAX(storage_schema_version, data_schema_version on memtable)
  int get_newest_schema_version(int64_t &schema_version) const;

  int submit_medium_compaction_clog(
      compaction::ObMediumCompactionInfo &medium_info,
      ObIAllocator &allocator);
  int replay_medium_compaction_clog(
      const share::SCN &scn,
      const char *buf,
      const int64_t buf_size,
      int64_t &pos);

  int fetch_tablet_autoinc_seq_cache(
      const uint64_t cache_size,
      share::ObTabletAutoincInterval &result);

  int update_tablet_autoinc_seq(const uint64_t autoinc_seq);
  int get_kept_snapshot_info(
      const int64_t min_reserved_snapshot_on_ls,
      ObStorageSnapshotInfo &snapshot_info) const;
  int check_schema_version_elapsed(
      const int64_t schema_version,
      const bool need_wait_trans_end,
      int64_t &max_commit_version,
      transaction::ObTransID &pending_tx_id);
  int replay_schema_version_change_log(const int64_t schema_version);
  int get_tablet_report_info(
      const int64_t snapshot_version,
      common::ObIArray<int64_t> &column_checksums,
      int64_t &data_size,
      int64_t &required_size,
      const bool need_checksums = true);
  int check_and_set_initial_state();
  int read_mds_table(
      common::ObIAllocator &allocator,
      ObTabletMdsData &mds_data,
      const bool for_flush,
      const int64_t mds_construct_sequence = 0) const;
  int get_mds_table_for_dump(mds::MdsTableHandle &mds_table) const;
  int64_t get_memtable_count() const { return memtable_count_; }

  int check_new_mds_with_cache(const int64_t snapshot_version, const int64_t timeout);
  int check_tablet_status_for_read_all_committed();
  int check_schema_version_with_cache(const int64_t schema_version, const int64_t timeout);
  int check_snapshot_readable_with_cache(
      const int64_t snapshot_version,
      const int64_t schema_version,
      const int64_t timeout);
  int set_tablet_status(
      const ObTabletCreateDeleteMdsUserData &tablet_status,
      mds::MdsCtx &ctx);
  int replay_set_tablet_status(
      const share::SCN &scn,
      const ObTabletCreateDeleteMdsUserData &tablet_status,
      mds::MdsCtx &ctx);
  int set_ddl_info(
      const ObTabletBindingMdsUserData &ddl_info,
      mds::MdsCtx &ctx,
      const int64_t lock_timeout_us);
  int replay_set_ddl_info(
      const share::SCN &scn,
      const ObTabletBindingMdsUserData &ddl_info,
      mds::MdsCtx &ctx);


  int set_frozen_for_all_memtables();
  // different from the is_valid() function
  // typically used for check valid for migration or restore
  int check_valid(const bool ignore_ha_status = false) const;

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int get_max_column_cnt_on_schema_recorder(int64_t &max_column_cnt);
  static int get_tablet_block_header_version(const char *buf, const int64_t len, int32_t &version);
  int get_all_minor_sstables(ObTableStoreIterator &table_store_iter) const;
  int get_sstable_read_info(
      const blocksstable::ObSSTable *sstable,
      const storage::ObITableReadInfo *&index_read_info) const;
  int build_full_memory_mds_data(
      common::ObArenaAllocator &allocator,
      ObTabletFullMemoryMdsData &data) const;
  int get_memtables(
      common::ObIArray<ObTableHandleV2> &memtables,
      const bool need_active) const;

  int set_macro_block(
      const ObDDLMacroBlock &macro_block,
      const int64_t snapshot_version,
      const uint64_t data_format_version);
protected:// for MDS use
  virtual bool check_is_inited_() const override final { return is_inited_; }
  virtual const ObTabletMeta &get_tablet_meta_() const override final { return tablet_meta_; }
  virtual int get_mds_table_handle_(mds::MdsTableHandle &handle,
                                    const bool create_if_not_exist) const override final;
  virtual ObTabletPointer *get_tablet_pointer_() const override final;
private:
  int update_meta_last_persisted_committed_tablet_status_from_sstable(
      const ObUpdateTableStoreParam &param,
      const ObTabletCreateDeleteMdsUserData &last_tablet_status);
  int update_tablet_status_from_sstable();
  int partial_deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos);
  int get_sstables_size(const bool ignore_shared_block, int64_t &used_size) const;
  static int deserialize_macro_info(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos,
      ObTabletMacroInfo *&tablet_macro_info);
  int init_aggregated_info(common::ObArenaAllocator &allocator, ObLinkedMacroBlockItemWriter &linked_writer);
  void set_initial_addr();
  int check_meta_addr() const;
  static int parse_meta_addr(const ObMetaDiskAddr &addr, ObIArray<blocksstable::MacroBlockId> &meta_ids);
  void dec_ref_with_aggregated_info();
  void dec_ref_without_aggregated_info();
  void dec_ref_with_macro_iter(ObMacroInfoIterator &macro_iter) const;
  int inner_inc_macro_ref_cnt();
  int inc_ref_with_aggregated_info();
  int inc_ref_without_aggregated_info();
  int inc_ref_with_macro_iter(ObMacroInfoIterator &macro_iter, bool &inc_success) const;
  void dec_table_store_ref_cnt();
  int inc_table_store_ref_cnt(bool &inc_success);
  static int inc_addr_ref_cnt(const ObMetaDiskAddr &addr, bool &inc_success);
  static void dec_addr_ref_cnt(const ObMetaDiskAddr &addr);
  static int inc_linked_block_ref_cnt(const ObMetaDiskAddr &head_addr, bool &inc_success);
  static void dec_linked_block_ref_cnt(const ObMetaDiskAddr &head_addr);
  int64_t get_try_cache_size() const;
  int inner_release_memtables(const share::SCN scn);
  int calc_sstable_occupy_size(int64_t &occupy_size);
  inline void set_space_usage_(const ObTabletSpaceUsage &space_usage) { tablet_meta_.set_space_usage_(space_usage); }
private:
  static bool ignore_ret(const int ret);
  int inner_check_valid(const bool ignore_ha_status = false) const;
  int self_serialize(char *buf, const int64_t len, int64_t &pos) const;
  int64_t get_self_serialize_size() const;
  static int check_schema_version(const ObDDLInfoCache& ddl_info_cache, const int64_t schema_version);
  static int check_snapshot_readable(const ObDDLInfoCache& ddl_info_cache, const int64_t snapshot_version, const int64_t schema_version);
  int get_column_store_sstable_checksum(common::ObIArray<int64_t> &column_checksums, ObCOSSTableV2 &co_sstable);

  logservice::ObLogHandler *get_log_handler() const { return log_handler_; } // TODO(bowen.gbw): get log handler from tablet pointer handle

  int init_shared_params(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t max_saved_schema_version,
      const int64_t max_saved_medium_scn,
      const lib::Worker::CompatMode compat_mode,
      ObFreezer *freezer);
  int build_read_info(common::ObArenaAllocator &allocator, const ObTablet *tablet = nullptr);
  int create_memtable(const int64_t schema_version,
                      const share::SCN clog_checkpoint_scn,
                      const bool for_direct_load,
                      const bool for_replay);
  int try_update_start_scn();
  int try_update_ddl_checkpoint_scn();
  int try_update_table_store_flag(const ObUpdateTableStoreParam &param);
  int get_max_schema_version(int64_t &schema_version);
  int inner_get_all_sstables(ObTableStoreIterator &iter, const bool need_unpack = false) const;
  int check_schema_version_for_bounded_staleness_read(
      const int64_t table_version_for_read,
      const int64_t data_max_schema_version,
      const uint64_t table_id);

  int do_rowkey_exists(
      ObTableIterParam &param,
      ObTableAccessContext &context,
      const blocksstable::ObDatumRowkey &rowkey,
      bool &exists);
  static int do_rowkeys_exist(
      ObTableStoreIterator &tables_iter,
      ObRowsInfo &rows_info,
      bool &exists);
  static int prepare_memtable(
      ObRelativeTable &relative_table,
      ObStoreCtx &store_ctx,
      memtable::ObMemtable *&write_memtable);

  int inner_create_memtable(
      const share::SCN clog_checkpoint_scn,
      const int64_t schema_version,
      const bool for_direct_load,
      const bool for_replay);

  int inner_get_memtables(common::ObIArray<storage::ObITable *> &memtables, const bool need_active) const;

  int write_sync_tablet_seq_log(share::ObTabletAutoincSeq &autoinc_seq,
                                share::SCN &scn);

  int update_ddl_info(
      const int64_t schema_version,
      const share::SCN &scn,
      int64_t &schema_refreshed_ts);
  int write_tablet_schema_version_change_clog(
      const int64_t schema_version,
      share::SCN &scn);
  int get_ddl_info(
      int64_t &refreshed_schema_version,
      int64_t &refreshed_schema_ts) const;
  int get_read_tables(
      const int64_t snapshot_version,
      ObTabletTableIterator &iter,
      const bool allow_no_ready_read);
  int get_read_major_sstable(
      const int64_t &major_snapshot_version,
      ObTabletTableIterator &iter);
  int auto_get_read_tables(
      const int64_t snapshot_version,
      ObTabletTableIterator &iter,
      const bool allow_no_ready_read);
  int get_read_tables_(
      const int64_t snapshot_version,
      ObTableStoreIterator &iter,
      ObStorageMetaHandle &table_store_handle,
      const bool allow_no_ready_read);
  int get_mds_tables(
      const int64_t snapshot_version,
      ObTableStoreIterator &iter) const;
  int get_read_major_sstable(
      const int64_t &major_snapshot_version,
      ObTableStoreIterator &iter) const;
  int allow_to_read_();

  int check_medium_list() const;
  int check_sstable_column_checksum() const;
  int get_finish_medium_scn(int64_t &finish_medium_scn) const;

  int inner_get_mds_table(
      mds::MdsTableHandle &mds_table,
      bool not_exist_create = false) const;
  int validate_medium_info_list(
      const int64_t finish_medium_scn,
      const ObTabletMdsData &mds_data) const;
  int read_medium_array(
      common::ObArenaAllocator &allocator,
      common::ObIArray<compaction::ObMediumCompactionInfo*> &medium_info_array) const;
  int set_initial_state(const bool initial_state);
  int set_macro_info_addr(
      const blocksstable::MacroBlockId &macro_id,
      const int64_t offset,
      const int64_t size,
      const ObMetaDiskAddr::DiskType block_type);

  int load_deserialize_v1(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos);
  int deserialize_meta_v1(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos,
      share::ObTabletAutoincSeq &autoinc_seq,
      ObTabletTxMultiSourceDataUnit &tx_data,
      ObTabletBindingInfo &ddl_data);
  int load_deserialize_v2(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos,
      const bool prepare_memtable = true /* whether to prepare memtable */);

  int load_deserialize_v3(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t len,
      int64_t &pos,
      const bool prepare_memtable);

  int get_src_tablet_read_tables_(
      const int64_t snapshot_version,
      const bool allow_no_ready_read,
      ObTabletTableIterator &iter,
      bool &succ_get_src_tables);

  int prepare_param(ObRelativeTable &relative_table, ObTableIterParam &param);
  int prepare_param_ctx(
      common::ObIAllocator &allocator,
      ObRelativeTable &relative_table,
      ObStoreCtx &ctx,
      ObTableIterParam &param,
      ObTableAccessContext &context);

#ifdef OB_BUILD_TDE_SECURITY
  void get_encrypt_meta(
      const uint64_t table_id,
      const common::ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr,
      const transaction::ObSerializeEncryptMeta *&encrypt_meta);
#endif

  // memtable operation
  int pull_memtables(ObArenaAllocator &allocator, ObDDLKV **&ddl_kvs_addr, int64_t &ddl_kv_count);
  int pull_memtables_without_ddl();
  int update_memtables();
  int build_memtable(common::ObIArray<ObTableHandleV2> &handle_array, const int64_t start_pos = 0);
  int rebuild_memtable(common::ObIArray<ObTableHandleV2> &handle_array);
  int rebuild_memtable(
      const share::SCN &clog_checkpoint_scn,
      common::ObIArray<ObTableHandleV2> &handle_array);
  int add_memtable(ObIMemtable* const table);
  bool exist_memtable_with_end_scn(const ObITable *table, const share::SCN &end_scn);
  int assign_memtables(ObIMemtable * const *memtables, const int64_t memtable_count);
  int assign_ddl_kvs(ObDDLKV * const *ddl_kvs, const int64_t ddl_kv_count);
  int pull_ddl_memtables(ObArenaAllocator &allocator, ObDDLKV **&ddl_kvs_addr, int64_t &ddl_kv_count);
  void reset_ddl_memtables();
  int wait_release_memtables_();
  int mark_mds_table_switched_to_empty_shell_();
  int handle_transfer_replace_(const ObBatchUpdateTableStoreParam &param);
  // NOTICE:
  // - Because the `calc_tablet_attr()` may has I/O operations, you can bypass it if wantn't to update it.
  int get_updating_tablet_pointer_param(
      ObUpdateTabletPointerParam &param,
      const bool need_tablet_attr = true) const;
  int calc_tablet_attr(ObTabletAttr &attr) const;
  int check_ready_for_read_if_need(const ObTablet &old_tablet);

  // mds mvs
  int build_mds_mini_sstable_for_migration(
      common::ObArenaAllocator &allocator,
      const ObMigrationTabletParam &param,
      ObTableHandleV2 &mds_mini_sstable);
  int build_migration_tablet_param_storage_schema(
      ObMigrationTabletParam &mig_tablet_param) const;
  int build_migration_tablet_param_last_tablet_status(
      ObMigrationTabletParam &mig_tablet_param) const;

  int build_transfer_tablet_param_current_(
      const share::ObLSID &dest_ls_id,
      ObMigrationTabletParam &mig_tablet_param);

  int clear_memtables_on_table_store(); // be careful to call this func, will destroy memtables array on table_store
public:
  static constexpr int32_t VERSION_V1 = 1;
  static constexpr int32_t VERSION_V2 = 2;
  static constexpr int32_t VERSION_V3 = 3;
  static constexpr int32_t VERSION_V4 = 4;
private:
  // ObTabletDDLKvMgr::MAX_DDL_KV_CNT_IN_STORAGE
  // Array size is too large, need to shrink it if possible
  static const int64_t DDL_KV_ARRAY_SIZE = 64;
  static const int64_t SHARED_MACRO_BUCKET_CNT = 100;
  static const int64_t MAX_PRINT_COUNT = 100;
private:
  int32_t version_;
  int32_t length_;
  volatile int64_t wash_score_;
  ObTabletMdsData *mds_data_;
  volatile int64_t ref_cnt_;
  ObTabletHandle next_tablet_guard_;                         // size: 56B, alignment: 8B
  ObTabletMeta tablet_meta_;                                 // size: 248, alignment: 8B
  ObRowkeyReadInfo *rowkey_read_info_;
  // in memory or disk
  ObTabletComplexAddr<ObTabletTableStore> table_store_addr_; // size: 48B, alignment: 8B
  // always in disk
  ObTabletComplexAddr<ObStorageSchema> storage_schema_addr_; // size: 48B, alignment: 8B
  ObTabletComplexAddr<ObTabletMacroInfo> macro_info_addr_;   // size: 48B, alignment: 8B
  int64_t memtable_count_;
  ObDDLKV **ddl_kvs_;
  int64_t ddl_kv_count_;
  ObTabletPointerHandle pointer_hdl_;                        // size: 24B, alignment: 8B
  ObMetaDiskAddr tablet_addr_;                               // size: 40B, alignment: 8B
  // NOTICE: these two pointers: memtable_mgr_ and log_handler_,
  // are considered as cache for tablet.
  // we keep it on tablet because we cannot get them in ObTablet::deserialize
  // through ObTabletPointerHandle.
  // may be some day will fix this issue, then the pointers have no need to exist.
  // won't persist
  ObIMemtable *memtables_[MAX_MEMSTORE_CNT];
  ObArenaAllocator *allocator_;
  mutable common::SpinRWLock memtables_lock_;                // size: 12B, alignment: 4B
  logservice::ObLogHandler *log_handler_;

  //ATTENTION : Add a new variable need consider ObMigrationTabletParam
  // and tablet meta init interface for migration.
  // yuque :
  ObTablet *next_tablet_; // used in old_version_chain and tablet_gc_queue

  // whether hold ref cnt
  // when destroying tablet, only if hold_ref_cnt_ is true do we decrease meta blocks' ref cnt
  // we need to set it to true after increasing meta blocks' ref cnt or deserializing tablet
  bool hold_ref_cnt_;
  bool is_inited_;
  mutable common::SpinRWLock mds_cache_lock_;                // size: 12B, alignment: 4B
  ObTabletStatusCache tablet_status_cache_;                  // size: 24B, alignment: 8B
  ObDDLInfoCache ddl_data_cache_;                            // size: 24B, alignment: 8B
  ObTableStoreCache table_store_cache_; // no need to serialize, should be initialized after table store is initialized.
};

inline bool ObTablet::is_ls_inner_tablet() const
{
  return tablet_meta_.tablet_id_.is_ls_inner_tablet();
}

inline bool ObTablet::is_ls_tx_data_tablet() const
{
  return tablet_meta_.tablet_id_.is_ls_tx_data_tablet();
}

inline bool ObTablet::is_ls_tx_ctx_tablet() const
{
  return tablet_meta_.tablet_id_.is_ls_tx_ctx_tablet();
}

inline bool ObTablet::is_valid() const
{
  return (!is_empty_shell()
          && pointer_hdl_.is_valid()
          && tablet_meta_.is_valid()
          && table_store_addr_.is_valid()
          && storage_schema_addr_.is_valid()
          && nullptr != rowkey_read_info_)
          || (is_empty_shell()
          && table_store_addr_.addr_.is_none()
          && storage_schema_addr_.addr_.is_none()
          && nullptr == rowkey_read_info_);
}

inline int ObTablet::allow_to_read_()
{
  return tablet_meta_.ha_status_.is_none() ? common::OB_SUCCESS : common::OB_REPLICA_NOT_READABLE;
}

inline void ObTablet::update_wash_score(const int64_t score)
{
  int64_t ret_v = 0;
  int64_t old_v = ATOMIC_LOAD(&wash_score_);
  if (score > old_v) {
    while (old_v != (ret_v = ATOMIC_CAS(&wash_score_, old_v, score))) {
      if (ret_v >= score) {
        break; // higher score may be updated by others, so just skip.
      } else {
        old_v = ret_v;
      }
    }
  }
}

inline void ObTablet::inc_ref()
{
  const int64_t cnt = ATOMIC_AAF(&ref_cnt_, 1);
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  STORAGE_LOG(DEBUG, "tablet inc ref", KP(this), K(tablet_id), "ref_cnt", cnt, K(lbt()));
}

inline int64_t ObTablet::dec_ref()
{
  const int64_t cnt = ATOMIC_SAF(&ref_cnt_, 1/* just sub 1 */);
  const common::ObTabletID &tablet_id = tablet_meta_.tablet_id_;
  STORAGE_LOG(DEBUG, "tablet dec ref", KP(this), K(tablet_id), "ref_cnt", cnt, K(lbt()));

  return cnt;
}

#ifdef OB_BUILD_TDE_SECURITY
inline void ObTablet::get_encrypt_meta(
     const uint64_t table_id,
     const common::ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr,
     const transaction::ObSerializeEncryptMeta *&encrypt_meta)
{
  for (int64_t i = 0; i < encrypt_meta_arr->count(); ++i) {
    if (encrypt_meta_arr->at(i).real_table_id() == table_id) {
      encrypt_meta = &(encrypt_meta_arr->at(i).meta_);
      break;
    }
  }
}
#endif

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TABLET_OB_TABLET
