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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_
#include "share/allocator/ob_gmemstore_allocator.h"

#include "share/ob_tenant_mgr.h"
#include "share/ob_cluster_version.h"
#include "lib/worker.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/ob_row_compactor.h"
#include "storage/memtable/ob_multi_source_data.h"
#include "storage/checkpoint/ob_freeze_checkpoint.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/tx_storage/ob_ls_handle.h" //ObLSHandle

namespace oceanbase
{
namespace common
{
class ObVersion;
class ObTabletID;
}
namespace storage
{
class ObTabletMemtableMgr;
class ObFreezer;
class ObStoreRowIterator;
}
namespace memtable
{
class ObMemtableScanIterator;
class ObMemtableGetIterator;

struct ObMtStat
{
  void reset() { memset(this, 0, sizeof(*this));}
  int64_t insert_row_count_;
  int64_t update_row_count_;
  int64_t delete_row_count_;
  int64_t purge_row_count_;
  int64_t purge_queue_count_;
  int64_t frozen_time_;
  int64_t ready_for_flush_time_;
  int64_t create_flush_dag_time_;
  int64_t release_time_;
  int64_t last_print_time_;
};

class ObMTKVBuilder
{
public:
  ObMTKVBuilder() {}
  virtual ~ObMTKVBuilder() {}
public:
  int dup_key(ObStoreRowkey *&new_key, common::ObIAllocator &alloc, const ObStoreRowkey *key)
  {
    int ret = OB_SUCCESS;
    new_key = NULL;
    if (OB_ISNULL(key)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid args", KP(key));
    } else if (OB_ISNULL(new_key = (ObStoreRowkey *)alloc.alloc(sizeof(ObStoreRowkey)))
               || OB_ISNULL(new(new_key) ObStoreRowkey())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc failed", KP(key), K(ret));
    } else if (OB_FAIL(key->deep_copy(*new_key, alloc))) {
      TRANS_LOG(WARN, "dup fail", K(key), K(ret));
      if (OB_NOT_NULL(new_key)) {
        alloc.free((void *)new_key);
        new_key = nullptr;
      }
    }
    return ret;
  }

  // template parameter only supports ObMemtableData and ObMemtableDataHeader,
  // actual return value is always the size of ObMemtableDataHeader
  template<class T>
  int get_data_size(const T *data, int64_t &data_size)
  {
    int ret = OB_SUCCESS;
    data_size = 0;
    if (data->buf_len_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "buf_len is invalid", KP(data));
    } else {
      data_size = data->dup_size();
    }
    return ret;
  }

  // template parameter only supports ObMemtableData and ObMemtableDataHeader,
  // actual dup objetc is always ObMemtableDataHeader
  template<class T>
  int dup_data(ObMvccTransNode *&new_node, common::ObIAllocator &allocator, const T *data)
  {
    int ret = OB_SUCCESS;
    int64_t data_size = 0;
    new_node = nullptr;
    if (OB_FAIL(get_data_size(data, data_size))) {
      TRANS_LOG(WARN, "get_data_size failed", K(ret), KP(data), K(data_size));
    } else if (OB_ISNULL(new_node = (ObMvccTransNode *)allocator.alloc(sizeof(ObMvccTransNode) + data_size))
               || OB_ISNULL(new(new_node) ObMvccTransNode())) {
      TRANS_LOG(WARN, "alloc ObMvccTransNode fail");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(ObMemtableDataHeader::build(reinterpret_cast<ObMemtableDataHeader *>(new_node->buf_), data))) {
      TRANS_LOG(WARN, "MemtableData dup fail", K(ret));
    }
    return ret;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObMTKVBuilder);
};

class ObMemtableState
{
public:
  static const int64_t INVALID = -1;
  static const int64_t ACTIVE = 0;
  static const int64_t MAJOR_FROZEN = 1;
  static const int64_t MINOR_FROZEN = 2;
  static const int64_t MAJOR_MERGING = 3;
  static const int64_t MINOR_MERGING = 4;
public:
  bool is_valid(const int64_t state) { return state >= ACTIVE && state <= MINOR_MERGING; }
};

class ObMemtableFreezeState
{
public:
  static const int64_t INVALID = -1;
  static const int64_t NOT_READY_FOR_FLUSH = 0;
  static const int64_t READY_FOR_FLUSH = 1;
  static const int64_t FLUSHED = 2;
  static const int64_t RELEASED = 3;
};

class ObMemtableMutatorIterator;
class ObEncryptRowBuf;

enum class MemtableRefOp
{
  NONE = 0,
  INC_REF,
  DEC_REF
};

class ObMemtable : public ObIMemtable, public storage::checkpoint::ObFreezeCheckpoint
{
public:
  typedef common::ObGMemstoreAllocator::AllocHandle ObMemstoreAllocator;
  ObMemtable();
  virtual ~ObMemtable();
public:
  int init(const ObITable::TableKey &table_key,
           ObLSHandle &ls_handle,
           storage::ObFreezer *freezer,
           storage::ObTabletMemtableMgr *memtable_mgr,
           const int64_t schema_version,
           const uint32_t freeze_clock);
  virtual void destroy();
  virtual int safe_to_destroy(bool &is_safe);

  OB_INLINE void reset() { destroy(); }
public:
  // ==================== Memtable Operation Interface ==================

  // set is used to insert/update the row
  // ctx is the writer tx's context, we need the tx_id, version and scn to do the concurrent control(mvcc_write)
  // tablet_id is necessary for the query_engine's key engine(NB: do we need it now?)
  // rowkey_len is the length of the row key in columns and new_row(NB: can we encapsulate it better?)
  // columns is the schema of the new_row, it both contains the row key and row value
  // update_idx is the index of the updated columns for update
  // old_row is the old version of the row for set action, it contains all columns(NB: it works for liboblog only currently)
  // new_row is the new version of the row for set action, it only contains the necessary columns for update and entire columns for insert
  virtual int set(
      storage::ObStoreCtx &ctx,
      const uint64_t table_id,
      const storage::ObTableReadInfo &read_info,
      const common::ObIArray<share::schema::ObColDesc> &columns, // TODO: remove columns
      const storage::ObStoreRow &row);
  virtual int set(
      storage::ObStoreCtx &ctx,
      const uint64_t table_id,
      const storage::ObTableReadInfo &read_info,
      const common::ObIArray<share::schema::ObColDesc> &columns, // TODO: remove columns
      const ObIArray<int64_t> &update_idx,
      const storage::ObStoreRow &old_row,
      const storage::ObStoreRow &new_row);

  // lock is used to lock the row(s)
  // ctx is the locker tx's context, we need the tx_id, version and scn to do the concurrent control(mvcc_write)
  // tablet_id is necessary for the query_engine's key engine(NB: do we need it now?)
  // columns is the schema of the new_row, it contains the row key
  // row/rowkey/row_iter is the row key or row key iterator for lock
  virtual int lock(
      storage::ObStoreCtx &ctx,
      const uint64_t table_id,
      const storage::ObTableReadInfo &read_info,
      common::ObNewRowIterator &row_iter);
  virtual int lock(
      storage::ObStoreCtx &ctx,
      const uint64_t table_id,
      const storage::ObTableReadInfo &read_info,
      const common::ObNewRow &row);
  virtual int lock(
      storage::ObStoreCtx &ctx,
      const uint64_t table_id,
      const storage::ObTableReadInfo &read_info,
      const blocksstable::ObDatumRowkey &rowkey);

  // exist/prefix_exist is used to ensure the (prefix) existance of the row
  // ctx is the locker tx's context, we need the tx_id, version and scn to do the concurrent control(mvcc_write)
  // tablet_id is necessary for the query_engine's key engine(NB: do we need it now?)
  // rowkey is the row key used for read
  // columns is the schema of the new_row, it contains the row key
  // rows_info is the the above information for multiple rowkeys
  // is_exist returns the existance of (one of) the rowkey(must not be deleted)
  // has_found returns the existance of the rowkey(may be deleted)
  // all_rows_found returns the existance of all of the rowkey(may be deleted) or existance of one of the rowkey(must not be deleted)
  // may_exist returns the possible existance of the rowkey(may be deleted)
  virtual int exist(
      storage::ObStoreCtx &ctx,
      const uint64_t table_id,
      const storage::ObTableReadInfo &read_info,
      const blocksstable::ObDatumRowkey &rowkey,
      bool &is_exist,
      bool &has_found);
  virtual int exist(
      storage::ObRowsInfo &rows_info,
      bool &is_exist,
      bool &all_rows_found);

  // get/scan is used to read/scan the row
  // param is the memtable access parameter, we need the descriptor(column schema and so on) of row in order to read the value
  // ctx is the reader tx's context, we need the tx_id, version and scn to do the concurrent control(lock_for_read)
  // rowkey is the row key used for read
  // range is the row key range used for scan
  // row/row_iter is the versioned value/value iterator for read
  virtual int get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObDatumRowkey &rowkey,
      blocksstable::ObDatumRow &row);
  virtual int get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObDatumRowkey &rowkey,
      storage::ObStoreRowIterator *&row_iter) override;
  virtual int scan(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObDatumRange &range,
      storage::ObStoreRowIterator *&row_iter) override;

  // multi_get/multi_scan is used to read/scan multiple row keys/ranges for performance
  // param is the memtable access parameter, we need the descriptor(column schema and so on) of row in order to read the value
  // ctx is the reader tx's context, we need the tx_id, version and scn to do the concurrent control(lock_for_read)
  // rowkeys is the row keys used for read
  // ranges is the row key ranges used for scan
  // row/row_iter is the versioned value/value iterator for read
  virtual int multi_get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
      storage::ObStoreRowIterator *&row_iter) override;
  virtual int multi_scan(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const common::ObIArray<blocksstable::ObDatumRange> &ranges,
      storage::ObStoreRowIterator *&row_iter) override;

  // replay_row is used to replay rows in redo log for follower
  // ctx is the writer tx's context, we need the scn, tx_id for fulfilling the tx node
  // mmi is mutator iterator for replay
  // decrypt_buf is used for decryption
  virtual int replay_row(
      storage::ObStoreCtx &ctx,
      ObMemtableMutatorIterator *mmi,
      ObEncryptRowBuf &decrypt_buf);
  virtual int replay_schema_version_change_log(
      const int64_t schema_version);

  // check_row_locked_by_myself check whether lock is locked by myself
  // ctx is the locker tx's context, we need the tx_id for check
  // tablet_id is necessary for the query_engine's key engine(NB: do we need it now?)
  // columns is the schema of the new_row, it contains the row key
  // rowkey is the row key used for lock
  // locked returns whether lock is locked by myself
  int check_row_locked_by_myself(
    storage::ObStoreCtx &ctx,
    const uint64_t table_id,
    const storage::ObTableReadInfo &read_info,
    const blocksstable::ObDatumRowkey &rowkey,
    bool &locked);

  // // TODO: ==================== Memtable Other Interface ==================
  int set_freezer(storage::ObFreezer *handler);
  storage::ObFreezer *get_freezer() { return freezer_; }
  int get_ls_id(share::ObLSID &ls_id);
  void set_memtable_mgr(storage::ObTabletMemtableMgr *mgr) { memtable_mgr_ = mgr; }
  void set_freeze_clock(const uint32_t freeze_clock) { ATOMIC_STORE(&freeze_clock_, freeze_clock); }
  uint32_t get_freeze_clock() { return ATOMIC_LOAD(&freeze_clock_); }
  int set_emergency(const bool emergency);
  ObMtStat& get_mt_stat() { return mt_stat_; }
  int64_t get_size() const;
  int64_t get_occupied_size() const;
  inline bool not_empty() const { return INT64_MAX != get_protection_clock(); };
  void set_max_schema_version(const int64_t schema_version);
  virtual int64_t get_max_schema_version() const override;
  int row_compact(ObMvccRow *value, const bool for_replay, const share::SCN snapshot_version);
  int64_t get_hash_item_count() const;
  int64_t get_hash_alloc_memory() const;
  int64_t get_btree_item_count() const;
  int64_t get_btree_alloc_memory() const;
  virtual bool can_be_minor_merged() override;
  virtual int get_frozen_schema_version(int64_t &schema_version) const override;
  virtual bool is_frozen_memtable() const override;
  virtual bool is_active_memtable() const override;
  virtual bool is_inner_tablet() const { return key_.tablet_id_.is_inner_tablet(); }
  ObTabletID get_tablet_id() const { return key_.tablet_id_; }
  int set_snapshot_version(const share::SCN snapshot_version);
  int64_t get_memtable_state() const { return state_; }
  int64_t get_freeze_state() const { return freeze_state_; }
  int64_t get_protection_clock() const { return local_allocator_.get_protection_clock(); }
  int64_t get_retire_clock() const { return local_allocator_.get_retire_clock(); }
  int get_current_right_boundary(share::SCN &current_right_boundary);

  inline bool& get_read_barrier() { return read_barrier_; }
  inline void set_write_barrier() { write_barrier_ = true; }
  inline void unset_write_barrier() { write_barrier_ = false; }
  inline void set_read_barrier() { read_barrier_ = true; }
  virtual int64_t inc_write_ref() override { return inc_write_ref_(); }
  virtual int64_t dec_write_ref() override;
  virtual int64_t get_write_ref() const override { return ATOMIC_LOAD(&write_ref_cnt_); }
  inline void set_is_tablet_freeze() { is_tablet_freeze_ = true; }
  inline bool get_is_tablet_freeze() { return is_tablet_freeze_; }
  inline void set_is_force_freeze() { is_force_freeze_ = true; }
  inline bool get_is_force_freeze() { return is_force_freeze_; }
  inline void set_is_flushed() { is_flushed_ = true; }
  inline bool get_is_flushed() { return is_flushed_; }
  inline void unset_active_memtable_logging_blocked() { ATOMIC_STORE(&unset_active_memtable_logging_blocked_, true); }
  inline void set_resolve_active_memtable_left_boundary(bool flag) { ATOMIC_STORE(&resolve_active_memtable_left_boundary_, flag); }
  inline bool get_resolve_active_memtable_left_boundary() { return ATOMIC_LOAD(&resolve_active_memtable_left_boundary_); }
  void set_freeze_state(const int64_t state);
  int get_merge_priority_info(ObMergePriorityInfo &merge_priority_info) const;
  void set_minor_merged();
  int64_t get_minor_merged_time() const { return minor_merged_time_; }
  common::ObIAllocator &get_allocator() {return local_allocator_;}
  bool has_hotspot_row() const { return ATOMIC_LOAD(&contain_hotspot_row_); }
  void set_contain_hotspot_row() { return ATOMIC_STORE(&contain_hotspot_row_, true); }
  virtual int64_t get_upper_trans_version() const override;
  virtual int estimate_phy_size(const ObStoreRowkey* start_key, const ObStoreRowkey* end_key, int64_t& total_bytes, int64_t& total_rows) override;
  virtual int get_split_ranges(const ObStoreRowkey* start_key, const ObStoreRowkey* end_key, const int64_t part_cnt, common::ObIArray<common::ObStoreRange> &range_array) override;
  ObQueryEngine &get_query_engine() { return query_engine_; }
  ObMvccEngine &get_mvcc_engine() { return mvcc_engine_; }
  const ObMvccEngine &get_mvcc_engine() const { return mvcc_engine_; }
  OB_INLINE bool is_inited() const { return is_inited_;}

  /* freeze */
  virtual int set_frozen() override { local_allocator_.set_frozen(); return OB_SUCCESS; }
  virtual bool rec_scn_is_stable() override;
  virtual bool ready_for_flush() override;
  void print_ready_for_flush();
  virtual int flush(share::ObLSID ls_id) override;
  share::SCN get_rec_scn() { return rec_scn_.atomic_get(); }
  virtual bool is_frozen_checkpoint() const override { return is_frozen_memtable();}
  virtual bool is_active_checkpoint() const override { return is_active_memtable();}

  virtual OB_INLINE share::SCN get_end_scn() const
  {
    return key_.scn_range_.end_scn_;
  }
  virtual OB_INLINE share::SCN get_start_scn() const
  {
    return key_.scn_range_.start_scn_;
  }
  bool is_empty() const override
  {
    return get_end_scn() == get_start_scn() &&
      share::ObScnRange::MIN_SCN == get_max_end_scn();
  }
  int resolve_right_boundary();
  void resolve_left_boundary(share::SCN end_scn);
  int resolve_snapshot_version_();
  int resolve_max_end_scn_();
  share::SCN get_max_end_scn() const { return max_end_scn_.atomic_get(); }
  int set_rec_scn(share::SCN rec_scn);
  int set_start_scn(const share::SCN start_ts);
  int set_end_scn(const share::SCN freeze_ts);
  int set_max_end_scn(const share::SCN scn);
  inline int set_logging_blocked()
  {
    logging_blocked_start_time = common::ObTimeUtility::current_time();
    ATOMIC_STORE(&logging_blocked_, true);
    return OB_SUCCESS;
  }
  inline void unset_logging_blocked()
  {
    if (get_logging_blocked()) {
      ATOMIC_STORE(&logging_blocked_, false);
      int64_t cost_time = common::ObTimeUtility::current_time() - logging_blocked_start_time;
      TRANS_LOG(INFO, "the cost time of logging blocked: ", K(cost_time), K(this), K(key_.tablet_id_));
    }
  }
  inline bool get_logging_blocked() { return ATOMIC_LOAD(&logging_blocked_); }
  int64_t get_unsubmitted_cnt() const { return ATOMIC_LOAD(&unsubmitted_cnt_); }
  int inc_unsubmitted_cnt();
  int dec_unsubmitted_cnt();
  int64_t get_unsynced_cnt() { return ATOMIC_LOAD(&unsynced_cnt_); }
  void inc_unsynced_cnt();
  int dec_unsynced_cnt();
  void inc_unsubmitted_and_unsynced_cnt();
  void dec_unsubmitted_and_unsynced_cnt();
  virtual uint32_t get_freeze_flag() override;
  virtual OB_INLINE int64_t get_timestamp() const override { return timestamp_; }
  void inc_timestamp(const int64_t timestamp) { timestamp_ = MAX(timestamp_, timestamp + 1); }
  int get_active_table_ids(common::ObIArray<uint64_t> &table_ids);
  bool is_partition_memtable_empty(const uint64_t table_id) const;
  blocksstable::ObDatumRange &m_get_real_range(blocksstable::ObDatumRange &real_range,
                                        const blocksstable::ObDatumRange &range, const bool is_reverse) const;
  int get_tx_table_guard(storage::ObTxTableGuard &tx_table_guard);
  int set_migration_clog_checkpoint_scn(const share::SCN &clog_checkpoint_scn);
  share::SCN get_migration_clog_checkpoint_scn() { return migration_clog_checkpoint_scn_.atomic_get(); }
  int resolve_right_boundary_for_migration();
  void unset_logging_blocked_for_active_memtable();
  void resolve_left_boundary_for_active_memtable();

  /* multi source data operations */
  virtual int get_multi_source_data_unit(
      ObIMultiSourceDataUnit *multi_source_data_unit,
      ObIAllocator *allocator,
      const bool get_lastest = true);
  template<class T>
  int get_multi_source_data_unit_list(
      const T * const useless_unit,
      ObMultiSourceData::ObIMultiSourceDataUnitList &dst_list,
      ObIAllocator *allocator);
  bool has_multi_source_data_unit(const MultiSourceDataUnitType type) const;

  template<class T>
  int save_multi_source_data_unit(const T *const multi_source_data_unit,
                                  const share::SCN scn,
                                  const bool for_replay,
                                  const MemtableRefOp ref_op = MemtableRefOp::NONE,
                                  const bool is_callback = false);
  // Print stat data in log.
  // For memtable debug.
  int print_stat() const;
  int check_cleanout(bool &is_all_cleanout,
                     bool &is_all_delay_cleanout,
                     int64_t &count);
  int dump2text(const char *fname);
  INHERIT_TO_STRING_KV("ObITable", ObITable, KP(this), KP_(memtable_mgr), K_(timestamp), K_(state),
                       K_(freeze_clock), K_(max_schema_version), K_(write_ref_cnt), K_(local_allocator),
                       K_(unsubmitted_cnt), K_(unsynced_cnt),
                       K_(logging_blocked), K_(unset_active_memtable_logging_blocked), K_(resolve_active_memtable_left_boundary),
                       K_(contain_hotspot_row), K_(max_end_scn), K_(rec_scn), K_(snapshot_version), K_(migration_clog_checkpoint_scn),
                       K_(is_tablet_freeze), K_(is_force_freeze), K_(contain_hotspot_row),
                       K_(read_barrier), K_(is_flushed), K_(freeze_state),
                       K_(mt_stat_.frozen_time), K_(mt_stat_.ready_for_flush_time),
                       K_(mt_stat_.create_flush_dag_time), K_(mt_stat_.release_time),
                       K_(mt_stat_.last_print_time));
private:
  static const int64_t OB_EMPTY_MEMSTORE_MAX_SIZE = 10L << 20; // 10MB
  int mvcc_write_(storage::ObStoreCtx &ctx,
                  const ObMemtableKey *key,
                  const storage::ObTableReadInfo &read_info,
                  const ObTxNodeArg &arg,
                  bool &is_new_locked);
  int mvcc_replay_(storage::ObStoreCtx &ctx,
                   const ObMemtableKey *key,
                   const ObTxNodeArg &arg);

  int lock_row_on_frozen_stores_(
      storage::ObStoreCtx &ctx,
      const ObTxNodeArg &arg,
      const ObMemtableKey *key,
      ObMvccRow *value,
      const storage::ObTableReadInfo &read_info,
      ObMvccWriteResult &res);

  int remove_unused_callback_for_uncommited_txn_();

  void get_begin(ObMvccAccessCtx &ctx);
  void get_end(ObMvccAccessCtx &ctx, int ret);
  void scan_begin(ObMvccAccessCtx &ctx);
  void scan_end(ObMvccAccessCtx &ctx, int ret);
  void set_begin(ObMvccAccessCtx &ctx);
  void set_end(ObMvccAccessCtx &ctx, int ret);

  int check_standby_cluster_schema_condition_(storage::ObStoreCtx &ctx,
                                              const int64_t table_id,
                                              const int64_t table_version);
  int set_(storage::ObStoreCtx &ctx,
           const uint64_t table_id,
           const storage::ObTableReadInfo &read_info,
           const common::ObIArray<share::schema::ObColDesc> &columns,
           const storage::ObStoreRow &new_row,
           const storage::ObStoreRow *old_row,
           const common::ObIArray<int64_t> *update_idx);
  int lock_(storage::ObStoreCtx &ctx,
            const uint64_t table_id,
            const storage::ObTableReadInfo &read_info,
            const common::ObStoreRowkey &rowkey);
  int lock_(storage::ObStoreCtx &ctx,
            const uint64_t table_id,
            const storage::ObTableReadInfo &read_info,
            const common::ObStoreRowkey &rowkey,
            ObMemtableKey &mtk);
  int post_row_write_conflict_(ObMvccAccessCtx &acc_ctx,
                               const ObMemtableKey &row_key,
                               storage::ObStoreRowLockState &lock_state,
                               const int64_t last_compact_cnt,
                               const int64_t total_trans_node_count);
  bool ready_for_flush_();
  int64_t inc_write_ref_();
  int64_t dec_write_ref_();
  int64_t inc_unsubmitted_cnt_();
  int64_t dec_unsubmitted_cnt_();
  int64_t inc_unsynced_cnt_();
  int64_t dec_unsynced_cnt_();
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtable);
  bool is_inited_;
  storage::ObLSHandle ls_handle_;
  storage::ObFreezer *freezer_;
  storage::ObTabletMemtableMgr *memtable_mgr_;
  uint32_t freeze_clock_;
  ObMemstoreAllocator local_allocator_;
  ObMTKVBuilder kv_builder_;
  ObQueryEngine query_engine_;
  ObMvccEngine mvcc_engine_;
  mutable ObMtStat mt_stat_;
  int64_t max_schema_version_;  // to record the max schema version of all data
  int64_t pending_cb_cnt_; // number of transactions have to sync log
  int64_t unsubmitted_cnt_; // number of trans node to be submitted logs
  int64_t unsynced_cnt_; // number of trans node to be synced logs
  bool logging_blocked_; // flag whether the memtable can submit log, cannot submit if true
  int64_t logging_blocked_start_time; // record the start time of logging blocked
  bool unset_active_memtable_logging_blocked_;
  bool resolve_active_memtable_left_boundary_;
  share::SCN freeze_scn_;
  share::SCN max_end_scn_;
  share::SCN rec_scn_;
  int64_t state_;
  int64_t freeze_state_;
  int64_t timestamp_;
  share::SCN migration_clog_checkpoint_scn_;
  bool is_tablet_freeze_;
  bool is_force_freeze_;
  bool is_flushed_;
  bool read_barrier_ CACHE_ALIGNED;
  bool write_barrier_;
  int64_t write_ref_cnt_ CACHE_ALIGNED;
  lib::Worker::CompatMode mode_;
  int64_t minor_merged_time_;
  bool contain_hotspot_row_;
  ObMultiSourceData multi_source_data_;
  mutable common::TCRWLock multi_source_data_lock_;
};

template<class T>
int ObMemtable::save_multi_source_data_unit(const T *const multi_source_data_unit,
                                            const share::SCN scn,
                                            const bool for_replay,
                                            const MemtableRefOp ref_op,
                                            const bool is_callback)
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(multi_source_data_lock_);

  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(multi_source_data_unit)) {
    ret = common::OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid args", K(ret));
  } else if (OB_UNLIKELY(!multi_source_data_unit->is_valid())) {
    ret = common::OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "not valid", K(ret), KPC(multi_source_data_unit));
  } else {
    const MultiSourceDataUnitType &type = multi_source_data_unit->type();
    if (MemtableRefOp::INC_REF == ref_op) {
      const_cast<T*>(multi_source_data_unit)->inc_unsync_cnt_for_multi_data();
      TRANS_LOG(INFO, "unsync_cnt_for_multi_data inc", K(key_.tablet_id_), K(type), KPC(multi_source_data_unit));
    } else if (MemtableRefOp::DEC_REF == ref_op) {
      const_cast<T*>(multi_source_data_unit)->dec_unsync_cnt_for_multi_data();
      TRANS_LOG(INFO, "unsync_cnt_for_multi_data dec", K(key_.tablet_id_), K(type), KPC(multi_source_data_unit));
    }
    if (MemtableRefOp::INC_REF == ref_op) {
      inc_unsubmitted_and_unsynced_cnt();
    }
    if (OB_FAIL(multi_source_data_.save_multi_source_data_unit(multi_source_data_unit, is_callback))) {
      TRANS_LOG(WARN, "fail to save to memtable", K(ret), KPC(multi_source_data_unit), K(type), KPC(this));
      if (MemtableRefOp::INC_REF == ref_op) {
        const_cast<T*>(multi_source_data_unit)->dec_unsync_cnt_for_multi_data();
        dec_unsubmitted_and_unsynced_cnt();
        TRANS_LOG(INFO, "unsync_cnt_for_multi_data dec for rollback", K(key_.tablet_id_), K(type), KPC(multi_source_data_unit));
      } else if (MemtableRefOp::DEC_REF == ref_op) {
        const_cast<T*>(multi_source_data_unit)->inc_unsync_cnt_for_multi_data();
        TRANS_LOG(INFO, "unsync_cnt_for_multi_data inc for rollback", K(key_.tablet_id_), K(type), KPC(multi_source_data_unit));
      }
    } else {
      if (scn > get_start_scn() && scn < share::ObScnRange::MAX_SCN) {
        if (OB_FAIL(ret)) {
        }
        // skip updating max_end_scn of frozen memtable for commit/abort when replay clog.
        else if ((!for_replay || !is_callback)
                 && OB_FAIL(set_max_end_scn(scn))) {
          TRANS_LOG(WARN, "failed to set max_end_scn", K(ret), K(scn), KPC(this));
        } else if (OB_FAIL(set_rec_scn(scn))) {
          TRANS_LOG(WARN, "failed to set rec_scn", K(ret), K(scn), KPC(this));
        }
      } else if (share::SCN::invalid_scn() != scn && share::ObScnRange::MAX_SCN != scn) {
        ret = common::OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "invalid scn", K(ret), K(scn), KPC(this));
      }

      if (MemtableRefOp::DEC_REF == ref_op) {
        dec_unsubmitted_and_unsynced_cnt();
      }
    }
    TRANS_LOG(INFO, "memtable save multi source data unit", K(ret), K(scn), K(ref_op),
              KPC(multi_source_data_unit), K(type), KPC(this));
  }

  return ret;
}

template<class T>
int ObMemtable::get_multi_source_data_unit_list(
    const T * const useless_unit,
    ObMultiSourceData::ObIMultiSourceDataUnitList &dst_list,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(multi_source_data_lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(!multi_source_data_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "multi source data is invalid", K(ret));
  } else if (OB_FAIL(multi_source_data_.get_multi_source_data_unit_list(useless_unit, dst_list, allocator))) {
    TRANS_LOG(WARN, "fail to get multi source data unit", K(ret));
  }

  return ret;
}

typedef ObMemtable ObMemStore;

/*
 * Print memtable statistics when receiving a signal.
 * For debug use.
 */
class ObMemtableStat
{
public:
  ObMemtableStat();
  virtual ~ObMemtableStat();
  static ObMemtableStat &get_instance();
public:
  int register_memtable(ObMemtable *memtable);
  int unregister_memtable(ObMemtable *memtable);
public:
  int print_stat();
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableStat);
  ObSpinLock lock_;
  ObArray<ObMemtable *> memtables_;
};

class RowHeaderGetter
{
public:
  RowHeaderGetter() : modify_count_(0), acc_checksum_(0) {}
  ~RowHeaderGetter() {}
  uint32_t get_modify_count() const { return modify_count_; }
  uint32_t get_acc_checksum() const { return acc_checksum_; }
  int get();
private:
  uint32_t modify_count_;
  uint32_t acc_checksum_;
};

}
}

#endif //OCEANBASE_MEMTABLE_OB_MEMTABLE_
