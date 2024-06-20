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

#include "share/allocator/ob_memstore_allocator.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_cluster_version.h"
#include "lib/literals/ob_literals.h"
#include "lib/worker.h"

#include "storage/ob_i_tablet_memtable.h"
#include "storage/ob_i_memtable_mgr.h"
#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/checkpoint/ob_checkpoint_diagnose.h"

namespace oceanbase
{
namespace storage
{
class ObTabletMemtableMgr;
class ObFreezer;
class ObStoreRowIterator;
class ObRowState;
}
namespace compaction
{
class ObTabletMergeDagParam;
}
namespace memtable
{
class ObMemtableMutatorIterator;

// report the dml stat to ObOptStatMonitorManager
struct ObReportedDmlStat
{
  static constexpr int64_t REPORT_INTERVAL = 1_s;
  ObReportedDmlStat() { reset(); }
  ~ObReportedDmlStat() = default;
  void reset() {
    last_report_time_ = 0;
    insert_row_count_ = 0;
    update_row_count_ = 0;
    delete_row_count_ = 0;
    table_id_ = OB_INVALID_ID;
    is_reporting_ = false;
  }

  int64_t last_report_time_;
  int64_t insert_row_count_;
  int64_t update_row_count_;
  int64_t delete_row_count_;
  // record the table_id for report the residual dml stat when memtable freeze,
  // in which case the table_id can't be acquired
  int64_t table_id_;
  bool is_reporting_;

  TO_STRING_KV(K_(last_report_time), K_(insert_row_count),
      K_(update_row_count), K_(delete_row_count), K_(table_id), K_(is_reporting));
};

struct ObMvccRowAndWriteResult
{
  ObMvccRow *mvcc_row_;
  ObMvccWriteResult write_result_;
  TO_STRING_KV(K_(write_result), KP_(mvcc_row));
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

enum class MemtableRefOp
{
  NONE = 0,
  INC_REF,
  DEC_REF
};

class ObMemtable : public ObITabletMemtable
{
public:
  using ObMvccRowAndWriteResults = common::ObSEArray<ObMvccRowAndWriteResult, 16>;
  typedef share::ObMemstoreAllocator::AllocHandle ObSingleMemstoreAllocator;

struct TabletMemtableUpdateFreezeInfo
{
public:
  TabletMemtableUpdateFreezeInfo(ObMemtable &memtable) : memtable_(memtable) {}
  TabletMemtableUpdateFreezeInfo& operator=(const TabletMemtableUpdateFreezeInfo&) = delete;
  void operator()(const checkpoint::ObCheckpointDiagnoseParam& param) const
  {
    checkpoint::ObCheckpointDiagnoseMgr *cdm = MTL(checkpoint::ObCheckpointDiagnoseMgr*);
    if (OB_NOT_NULL(cdm)) {
      cdm->update_freeze_info(param, memtable_.get_rec_scn(),
       memtable_.get_start_scn(), memtable_.get_end_scn(), memtable_.get_btree_alloc_memory());
    }
  }
private:
  ObMemtable &memtable_;
};

struct UpdateMergeInfoForMemtable
{
public:
  UpdateMergeInfoForMemtable(int64_t merge_start_time,
    int64_t merge_finish_time,
    int64_t occupy_size,
    int64_t concurrent_cnt)
    : merge_start_time_(merge_start_time),
      merge_finish_time_(merge_finish_time),
      occupy_size_(occupy_size),
      concurrent_cnt_(concurrent_cnt)
  {}
  UpdateMergeInfoForMemtable& operator=(const UpdateMergeInfoForMemtable&) = delete;
  void operator()(const checkpoint::ObCheckpointDiagnoseParam& param) const
  {
    checkpoint::ObCheckpointDiagnoseMgr *cdm = MTL(checkpoint::ObCheckpointDiagnoseMgr*);
    if (OB_NOT_NULL(cdm)) {
      cdm->update_merge_info_for_memtable(param, merge_start_time_, merge_finish_time_,
          occupy_size_, concurrent_cnt_);
    }
  }
private:
  int64_t merge_start_time_;
  int64_t merge_finish_time_;
  int64_t occupy_size_;
  int64_t concurrent_cnt_;
};

public:
  ObMemtable();
  virtual ~ObMemtable();
  OB_INLINE void reset() { destroy(); }
  virtual void destroy();

public: // derived from ObFreezeCheckpoint
  virtual bool rec_scn_is_stable() override;
  virtual bool ready_for_flush() override;

public: // derived from ObITabletMemtable
  virtual int init(const ObITable::TableKey &table_key,
                   ObLSHandle &ls_handle,
                   storage::ObFreezer *freezer,
                   storage::ObTabletMemtableMgr *memtable_mgr,
                   const int64_t schema_version,
                   const uint32_t freeze_clock) override;
  virtual void print_ready_for_flush() override;
  virtual void set_allow_freeze(const bool allow_freeze) override;
  virtual int set_frozen() override { local_allocator_.set_frozen(); return OB_SUCCESS; }
  virtual bool is_inited() const override { return is_inited_; }
  virtual int64_t dec_write_ref() override;
  virtual bool is_frozen_memtable() override;
  virtual int get_schema_info(
    const int64_t input_column_cnt,
    int64_t &max_schema_version_on_memtable,
    int64_t &max_column_cnt_on_memtable) const override;

public: // derived from ObITable
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
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const common::ObIArray<share::schema::ObColDesc> &columns, // TODO: remove columns
      const storage::ObStoreRow &row,
      const share::ObEncryptMeta *encrypt_meta,
      const bool check_exist);
  virtual int set(
      const storage::ObTableIterParam &param,
	    storage::ObTableAccessContext &context,
      const common::ObIArray<share::schema::ObColDesc> &columns, // TODO: remove columns
      const ObIArray<int64_t> &update_idx,
      const storage::ObStoreRow &old_row,
      const storage::ObStoreRow &new_row,
      const share::ObEncryptMeta *encrypt_meta);
  int multi_set(
      const storage::ObTableIterParam &param,
	    storage::ObTableAccessContext &context,
      const common::ObIArray<share::schema::ObColDesc> &columns,
      const storage::ObStoreRow *rows,
      const int64_t row_count,
      const bool check_exist,
      const share::ObEncryptMeta *encrypt_meta,
      storage::ObRowsInfo &rows_info);
  int check_rows_locked(
      const bool check_exist,
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      ObRowsInfo &rows_info);
  int check_rows_locked_on_ddl_merge_sstable(
      blocksstable::ObSSTable *sstable,
      const bool check_exist,
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      ObRowsInfo &rows_info);


  // lock is used to lock the row(s)
  // ctx is the locker tx's context, we need the tx_id, version and scn to do the concurrent control(mvcc_write)
  // tablet_id is necessary for the query_engine's key engine(NB: do we need it now?)
  // columns is the schema of the new_row, it contains the row key
  // row/rowkey/row_iter is the row key or row key iterator for lock

  virtual int lock(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const common::ObNewRow &row);
  virtual int lock(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
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
      const storage::ObTableIterParam &param,
	  storage::ObTableAccessContext &context,
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
      const share::SCN &scn,
      ObMemtableMutatorIterator *mmi);
  virtual int safe_to_destroy(bool &is_safe);

public:
  int64_t get_size() const;
  int64_t get_occupied_size() const;
  int64_t get_physical_row_cnt() const { return query_engine_.btree_size(); }
  inline bool not_empty() const { return INT64_MAX != get_protection_clock(); };
  void set_max_data_schema_version(const int64_t schema_version);
  int64_t get_max_data_schema_version() const;
  void set_max_column_cnt(const int64_t column_cnt);
  int64_t get_max_column_cnt() const;
  int row_compact(ObMvccRow *value,
                  const share::SCN snapshot_version,
                  const int64_t flag);
  int64_t get_hash_item_count() const;
  int64_t get_hash_alloc_memory() const;
  int64_t get_btree_item_count() const;
  int64_t get_btree_alloc_memory() const;

  virtual int get_frozen_schema_version(int64_t &schema_version) const override;
  virtual bool is_inner_tablet() const { return key_.tablet_id_.is_inner_tablet(); }
  int set_snapshot_version(const share::SCN snapshot_version);
  int64_t get_memtable_state() const { return state_; }
  int64_t get_protection_clock() const { return local_allocator_.get_protection_clock(); }
  int64_t get_retire_clock() const { return local_allocator_.get_retire_clock(); }

  void set_minor_merged();
  int64_t get_minor_merged_time() const { return minor_merged_time_; }
  common::ObIAllocator &get_allocator() {return local_allocator_;}
  bool has_hotspot_row() const { return ATOMIC_LOAD(&contain_hotspot_row_); }
  void set_contain_hotspot_row() { return ATOMIC_STORE(&contain_hotspot_row_, true); }
  virtual int64_t get_upper_trans_version() const override;
  virtual int estimate_phy_size(const ObStoreRowkey* start_key, const ObStoreRowkey* end_key, int64_t& total_bytes, int64_t& total_rows) override;
  virtual int get_split_ranges(const ObStoreRange &input_range,
                               const int64_t part_cnt,
                               ObIArray<ObStoreRange> &range_array) override;
  int split_ranges_for_sample(const blocksstable::ObDatumRange &table_scan_range,
                              const double sample_rate_percentage,
                              ObIAllocator &allocator,
                              ObIArray<blocksstable::ObDatumRange> &sample_memtable_ranges);

  ObQueryEngine &get_query_engine() { return query_engine_; }
  ObMvccEngine &get_mvcc_engine() { return mvcc_engine_; }
  const ObMvccEngine &get_mvcc_engine() const { return mvcc_engine_; }
  void pre_batch_destroy_keybtree();
  static int batch_remove_unused_callback_for_uncommited_txn(
    const share::ObLSID ls_id,
    const memtable::ObMemtableSet *memtable_set);

  /* freeze */
  virtual int flush(share::ObLSID ls_id) override;

  bool is_empty() const override
  {
    return ObITable::get_end_scn() == ObITable::get_start_scn() &&
      share::ObScnRange::MIN_SCN == get_max_end_scn();
  }
  void fill_compaction_param_(
    const int64_t current_time,
    compaction::ObTabletMergeDagParam &param);
  int resolve_snapshot_version_();
  int resolve_max_end_scn_();
  // User should take response of the recommend scn. All version smaller than
  // recommend scn should belong to the tables before the memtable and the
  // memtable. And under exception case, user need guarantee all new data is
  // bigger than the recommend_scn.
  inline void set_transfer_freeze(const share::SCN recommend_scn)
  {
    recommend_snapshot_version_.atomic_set(recommend_scn);
    ATOMIC_STORE(&transfer_freeze_flag_, true);
  }
  inline bool is_transfer_freeze() const { return ATOMIC_LOAD(&transfer_freeze_flag_); }
  virtual uint32_t get_freeze_flag() override;
  blocksstable::ObDatumRange &m_get_real_range(blocksstable::ObDatumRange &real_range,
                                        const blocksstable::ObDatumRange &range, const bool is_reverse) const;
  int get_tx_table_guard(storage::ObTxTableGuard &tx_table_guard);

#ifdef OB_BUILD_TDE_SECURITY
  /*clog encryption related*/
  int save_encrypt_meta(const uint64_t table_id, const share::ObEncryptMeta *encrypt_meta);
  int get_encrypt_meta(transaction::ObTxEncryptMeta *&encrypt_meta);
  bool need_for_save(const share::ObEncryptMeta *encrypt_meta);
#endif

  // Print stat data in log.
  // For memtable debug.
  int print_stat() const;
  int check_cleanout(bool &is_all_cleanout,
                     bool &is_all_delay_cleanout,
                     int64_t &count);
  virtual int dump2text(const char *fname) override;
  // TODO(handora.qc) ready_for_flush interface adjustment

  virtual int finish_freeze();

  INHERIT_TO_STRING_KV("ObITabletMemtable",
                       ObITabletMemtable,
                       KP(this),
                       K_(state),
                       K_(max_data_schema_version),
                       K_(max_column_cnt),
                       K_(local_allocator),
                       K_(contain_hotspot_row),
                       K_(snapshot_version),
                       K_(contain_hotspot_row),
                       K_(ls_id),
                       K_(transfer_freeze_flag),
                       K_(recommend_snapshot_version));
private:
  static const int64_t OB_EMPTY_MEMSTORE_MAX_SIZE = 10L << 20; // 10MB

  int get_all_tables_(ObStoreCtx &ctx, ObIArray<ObITable *> &iter_tables);
  int mvcc_write_(
      const storage::ObTableIterParam &param,
	    storage::ObTableAccessContext &context,
	    const ObMemtableKey *key,
	    const ObTxNodeArg &arg,
      const bool check_exist,
	    bool &is_new_locked,
      ObMvccRowAndWriteResult *mvcc_row = nullptr);

  int mvcc_replay_(storage::ObStoreCtx &ctx,
                   const ObMemtableKey *key,
                   const ObTxNodeArg &arg);
  int lock_row_on_frozen_stores_(
      const storage::ObTableIterParam &param,
      const ObTxNodeArg &arg,
      const ObMemtableKey *key,
      const bool check_exist,
      storage::ObTableAccessContext &context,
      ObMvccRow *value,
      ObMvccWriteResult &res);

  int lock_row_on_frozen_stores_on_success(
      const bool row_locked,
      const blocksstable::ObDmlFlag writer_dml_flag,
      const share::SCN &max_trans_version,
      storage::ObTableAccessContext &context,
      ObMvccRow *value,
      ObMvccWriteResult &res);

  void lock_row_on_frozen_stores_on_failure(
      const blocksstable::ObDmlFlag writer_dml_flag,
      const ObMemtableKey &key,
      int &ret,
      ObMvccRow *value,
      storage::ObTableAccessContext &context,
      ObMvccWriteResult &res);

  int lock_rows_on_frozen_stores_(
      const bool check_exist,
      const storage::ObTableIterParam &param,
      const ObMemtableKeyGenerator &memtable_keys,
      storage::ObTableAccessContext &context,
      ObMvccRowAndWriteResults &mvcc_rows,
      ObRowsInfo &rows_info);

  int internal_lock_row_on_frozen_stores_(const bool check_exist,
                                          const ObMemtableKey *key,
                                          const storage::ObTableIterParam &param,
                                          const ObIArray<ObITable *> &iter_tables,
                                          storage::ObTableAccessContext &context,
                                          ObMvccWriteResult &res,
                                          ObRowState &row_state);

  int internal_lock_rows_on_frozen_stores_(
      const bool check_exist,
      const ObIArray<ObITable *> &iter_tables,
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      share::SCN &max_trans_version,
      ObRowsInfo &rows_info);

  void get_begin(ObMvccAccessCtx &ctx);
  void get_end(ObMvccAccessCtx &ctx, int ret);
  void scan_begin(ObMvccAccessCtx &ctx);
  void scan_end(ObMvccAccessCtx &ctx, int ret);
  void set_begin(ObMvccAccessCtx &ctx);
  void set_end(ObMvccAccessCtx &ctx, int ret);

  int check_standby_cluster_schema_condition_(storage::ObStoreCtx &ctx,
                                              const int64_t table_id,
                                              const int64_t table_version);
  int set_(
      const storage::ObTableIterParam &param,
      const common::ObIArray<share::schema::ObColDesc> &columns,
      const storage::ObStoreRow &new_row,
      const storage::ObStoreRow *old_row,
      const common::ObIArray<int64_t> *update_idx,
      const ObMemtableKey &mtk,
      const bool check_exist,
      storage::ObTableAccessContext &context,
      ObMvccRowAndWriteResult *mvcc_row = nullptr);
  int multi_set_(
      const storage::ObTableIterParam &param,
      const common::ObIArray<share::schema::ObColDesc> &columns,
      const storage::ObStoreRow *rows,
      const int64_t row_count,
      const bool check_exist,
      const ObMemtableKeyGenerator &memtable_keys,
      storage::ObTableAccessContext &context,
      storage::ObRowsInfo &rows_info);
  int lock_(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const common::ObStoreRowkey &rowkey,
      const ObMemtableKey &mtk);

  int post_row_write_conflict_(ObMvccAccessCtx &acc_ctx,
                               const ObMemtableKey &row_key,
                               storage::ObStoreRowLockState &lock_state,
                               const int64_t last_compact_cnt,
                               const int64_t total_trans_node_count);
  bool ready_for_flush_();
  int64_t try_split_range_for_sample_(const ObStoreRange &input_range,
                                      const int64_t range_count,
                                      ObIAllocator &allocator,
                                      ObIArray<blocksstable::ObDatumRange> &sample_memtable_ranges);
  int try_report_dml_stat_(const int64_t table_id);
  int report_residual_dml_stat_();

private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtable);
  bool is_inited_;
  storage::ObLSHandle ls_handle_;
  ObSingleMemstoreAllocator local_allocator_;
  ObMTKVBuilder kv_builder_;
  ObQueryEngine query_engine_;
  ObMvccEngine mvcc_engine_;
  mutable ObReportedDmlStat reported_dml_stat_;
  int64_t max_data_schema_version_;  // to record the max schema version of write data
  // TODO(handora.qc): remove it as soon as possible
  // only used for decide special right boundary of memtable
  bool transfer_freeze_flag_;
  // only used for decide special snapshot version of memtable
  share::SCN recommend_snapshot_version_;

  int64_t state_;
  lib::Worker::CompatMode mode_;
  int64_t minor_merged_time_;
  bool contain_hotspot_row_;
  transaction::ObTxEncryptMeta *encrypt_meta_;
  common::SpinRWLock encrypt_meta_lock_;
  int64_t max_column_cnt_; // record max column count of row
};

}  // namespace memtable
}  // namespace oceanbase

#endif //OCEANBASE_MEMTABLE_OB_MEMTABLE_
