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

#include "common/ob_partition_key.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_cluster_version.h"
#include "share/ob_worker.h"

#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/ob_row_compactor.h"
#include "storage/ob_saved_storage_info_v2.h"

namespace oceanbase {
namespace common {
class ObStoreRange;
class ObVersion;
};  // namespace common
namespace memtable {
class ObMemtableCompactWriter;
class ObMemtableScanIterator;
class ObMemtableGetIterator;

struct ObMtStat {
  void reset()
  {
    memset(this, 0, sizeof(*this));
  }
  int64_t insert_row_count_;
  int64_t update_row_count_;
  int64_t delete_row_count_;
  int64_t purge_row_count_;
  int64_t purge_queue_count_;
};

class ObMTKVBuilder {
public:
  ObMTKVBuilder()
  {}
  virtual ~ObMTKVBuilder()
  {}

public:
  int dup_key(ObStoreRowkey*& new_key, common::ObIAllocator& alloc, const ObStoreRowkey* key)
  {
    int ret = OB_SUCCESS;
    new_key = NULL;
    if (OB_ISNULL(key)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid args", KP(key));
    } else if (OB_ISNULL(new_key = (ObStoreRowkey*)alloc.alloc(sizeof(ObStoreRowkey))) ||
               OB_ISNULL(new (new_key) ObStoreRowkey())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc failed", KP(key), K(ret));
    } else if (OB_FAIL(key->deep_copy(*new_key, alloc))) {
      TRANS_LOG(WARN, "dup fail", K(key), K(ret));
      if (OB_NOT_NULL(new_key)) {
        alloc.free((void*)new_key);
        new_key = nullptr;
      }
    }
    return ret;
  }

  // template parameter only supports ObMemtableData and ObMemtableDataHeader,
  // actual return value is always the size of ObMemtableDataHeader
  template <class T>
  int get_data_size(const T* data, int64_t& data_size)
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
  template <class T>
  int dup_data(ObMvccTransNode*& new_node, common::ObIAllocator& allocator, const T* data)
  {
    int ret = OB_SUCCESS;
    int64_t data_size = 0;
    new_node = nullptr;
    if (OB_FAIL(get_data_size(data, data_size))) {
      TRANS_LOG(WARN, "get_data_size failed", K(ret), KP(data), K(data_size));
    } else if (OB_ISNULL(new_node = (ObMvccTransNode*)allocator.alloc(sizeof(ObMvccTransNode) + data_size)) ||
               OB_ISNULL(new (new_node) ObMvccTransNode())) {
      TRANS_LOG(WARN, "alloc ObMvccTransNode fail");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(ObMemtableDataHeader::build(reinterpret_cast<ObMemtableDataHeader*>(new_node->buf_), data))) {
      TRANS_LOG(WARN, "MemtableData dup fail", K(ret));
    }
    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMTKVBuilder);
};

class ObMemtableState {
public:
  static const int64_t INVALID = -1;
  static const int64_t ACTIVE = 0;
  static const int64_t MAJOR_FROZEN = 1;
  static const int64_t MINOR_FROZEN = 2;
  static const int64_t MAJOR_MERGING = 3;
  static const int64_t MINOR_MERGING = 4;

public:
  bool is_valid(const int64_t state)
  {
    return state >= ACTIVE && state <= MINOR_MERGING;
  }
};

class ObMemtable : public ObIMemtable {
public:
  typedef common::ObGMemstoreAllocator::AllocHandle ObMemstoreAllocator;
  ObMemtable();
  virtual ~ObMemtable();

public:
  virtual int init(const ObITable::TableKey& table_key);
  virtual void destroy() override;
  int fake(const ObIMemtable& mt) override;

public:
  int dump2text(const char* fname);
  virtual int set(const storage::ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_len,
      const common::ObIArray<share::schema::ObColDesc>& columns, storage::ObStoreRowIterator& row_iter) override;
  virtual int set(const storage::ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_len,
      const common::ObIArray<share::schema::ObColDesc>& columns, const storage::ObStoreRow& row) override;
  virtual int set(const storage::ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_len,
      const common::ObIArray<share::schema::ObColDesc>& columns, const ObIArray<int64_t>& update_idx,
      const storage::ObStoreRow& old_row, const storage::ObStoreRow& new_row);
  virtual int lock(const storage::ObStoreCtx& ctx, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, common::ObNewRowIterator& row_iter) override;
  virtual int lock(const storage::ObStoreCtx& ctx, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, const common::ObNewRow& row) override;
  virtual int lock(const storage::ObStoreCtx& ctx, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, const common::ObStoreRowkey& rowkey) override;
  virtual int64_t get_upper_trans_version() const override;
  int check_row_locked_by_myself(const storage::ObStoreCtx& ctx, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, const common::ObStoreRowkey& rowkey, bool& locked);

public:
  int estimate_phy_size(const uint64_t table_id, const common::ObStoreRowkey* start_key,
      const common::ObStoreRowkey* end_key, int64_t& total_bytes, int64_t& total_rows);
  int get_split_ranges(const uint64_t table_id, const common::ObStoreRowkey* start_key,
      const common::ObStoreRowkey* end_key, const int64_t part_cnt,
      common::ObIArray<common::ObStoreRange>& range_array);
  virtual int exist(const storage::ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
      const common::ObIArray<share::schema::ObColDesc>& column_ids, bool& is_exist, bool& has_found) override;
  virtual int prefix_exist(storage::ObRowsInfo& rows_info, bool& may_exist) override;
  virtual int exist(storage::ObRowsInfo& rows_info, bool& is_exist, bool& all_rows_found) override;
  virtual int get(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const common::ObExtStoreRowkey& rowkey, storage::ObStoreRow& row) override;
  virtual int get(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const common::ObExtStoreRowkey& rowkey, storage::ObStoreRowIterator*& row_iter) override;
  virtual int scan(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const common::ObExtStoreRange& range, storage::ObStoreRowIterator*& row_iter) override;
  virtual int multi_get(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, storage::ObStoreRowIterator*& row_iter) override;
  virtual int multi_scan(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const common::ObIArray<common::ObExtStoreRange>& ranges, storage::ObStoreRowIterator*& row_iter) override;
  virtual int get_row_header(const storage::ObStoreCtx& ctx, const uint64_t table_id,
      const common::ObStoreRowkey& rowkey, const common::ObIArray<share::schema::ObColDesc>& column_ids,
      uint32_t& modify_count, uint32_t& acc_checksum);
  virtual int replay(const storage::ObStoreCtx& ctx, const char* data, const int64_t data_len);
  virtual int replay_schema_version_change_log(const int64_t schema_version);

  ObQueryEngine& get_query_engine()
  {
    return query_engine_;
  }
  ObMvccEngine& get_mvcc_engine()
  {
    return mvcc_engine_;
  }

  virtual int estimate_get_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, storage::ObPartitionEst& part_est) override;

  virtual int estimate_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObExtStoreRange& key_range, storage::ObPartitionEst& part_est) override;
  virtual int estimate_multi_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObIArray<common::ObExtStoreRange>& ranges, storage::ObPartitionEst& part_est) override;
  virtual int save_base_storage_info(const storage::ObSavedStorageInfoV2& info);
  virtual int get_base_storage_info(storage::ObSavedStorageInfoV2& info);
  int set_emergency(const bool emergency);
  virtual int minor_freeze(const bool emergency = false);
  ObMtStat& get_mt_stat()
  {
    return mt_stat_;
  }
  int64_t get_size() const;
  int64_t get_occupied_size() const;
  inline bool not_empty() const
  {
    return INT64_MAX != get_protection_clock();
  };
  int get_sdr(const ObMemtableKey* key, ObMemtableKey* start, ObMemtableKey* end, int64_t& max_version);
  void set_max_schema_version(const int64_t schema_version);
  int64_t get_max_schema_version() const;
  int update_max_trans_version(const int64_t trans_version);
  int64_t get_max_trans_version() const;
  int row_compact(ObMvccRow* value, const int64_t snapshot_version);
  int64_t get_hash_item_count() const;
  int64_t get_hash_alloc_memory() const;
  int64_t get_btree_item_count() const;
  int64_t get_btree_alloc_memory() const;
  int inc_active_trx_count();
  int dec_active_trx_count();
  int dec_pending_cb_count();
  int add_pending_cb_count(const int64_t cnt, const bool finish);
  void inc_pending_lob_count() override;
  void dec_pending_lob_count() override;
  void inc_pending_batch_commit_count();
  void dec_pending_batch_commit_count();
  void inc_pending_elr_count();
  void dec_pending_elr_count();
  int64_t get_active_trx_count() const
  {
    return ATOMIC_LOAD(&active_trx_count_);
  }
  int64_t get_pending_cb_count() const
  {
    return mark_finish_ ? pending_cb_cnt_ : INT64_MAX;
  }
  int64_t get_pending_lob_count() const
  {
    return mark_finish_ ? pending_lob_cnt_ : INT64_MAX;
  }
  bool can_be_minor_merged() const;
  virtual int get_frozen_schema_version(int64_t& schema_version) const override;
  virtual bool is_frozen_memtable() const override;
  virtual bool is_active_memtable() const override;
  virtual bool is_inner_table() const
  {
    return common::is_inner_table(key_.table_id_);
  }
  int set_snapshot_version(const int64_t snapshot_version);
  int set_base_version(const int64_t base_version);
  int set_start_log_ts(const int64_t start_ts);
  int set_end_log_ts(const int64_t freeze_ts);
  int update_max_log_ts(const int64_t log_ts);
  int row_relocate(const bool for_replay, const bool need_lock_for_write, const bool need_fill_redo, ObIMvccCtx& ctx,
      const ObMvccRowCallback* callback);
  int64_t get_memtable_state() const
  {
    return state_;
  }
  int64_t get_protection_clock() const
  {
    return local_allocator_.get_protection_clock();
  }
  int64_t get_retire_clock() const
  {
    return local_allocator_.get_retire_clock();
  }

  inline bool& get_read_barrier()
  {
    return read_barrier_;
  }
  inline void set_write_barrier()
  {
    write_barrier_ = true;
  }
  inline void unset_write_barrier()
  {
    write_barrier_ = false;
  }
  inline void set_read_barrier()
  {
    read_barrier_ = true;
  }
  int inc_write_ref();
  inline int64_t dec_write_ref()
  {
    return ATOMIC_SAF(&write_ref_cnt_, 1);
  }
  inline int64_t get_write_ref() const
  {
    return ATOMIC_LOAD(&write_ref_cnt_);
  }
  int get_merge_priority_info(ObMergePriorityInfo& merge_priority_info) const;
  void set_minor_merged();
  int64_t get_minor_merged_time() const
  {
    return minor_merged_time_;
  }
  common::ObIAllocator& get_allocator()
  {
    return local_allocator_;
  }
  bool has_hotspot_row() const
  {
    return ATOMIC_LOAD(&contain_hotspot_row_);
  }
  void set_contain_hotspot_row()
  {
    return ATOMIC_STORE(&contain_hotspot_row_, true);
  }

  int prepare_freeze_log_ts();
  void clear_freeze_log_ts();
  int64_t get_freeze_log_ts() const;
  bool frozen_log_applied() const
  {
    return frozen_log_applied_;
  }
  void log_applied(const int64_t applied_log_ts);
  void set_frozen()
  {
    frozen_ = true;
  }
  void set_with_accurate_log_ts_range(const bool with_accurate_log_ts_range);
  bool with_accurate_log_ts_range() const
  {
    return with_accurate_log_ts_range_;
  }
  virtual OB_INLINE int64_t get_timestamp() const override
  {
    return timestamp_;
  }
  void inc_timestamp(const int64_t timestamp)
  {
    timestamp_ = MAX(timestamp_, timestamp + 1);
  }
  void mark_for_split()
  {
    is_split_ = true;
  }
  int get_active_table_ids(common::ObIArray<uint64_t>& table_ids);

public:
  // Print stat data in log.
  // For memtable debug.
  int print_stat() const;
  INHERIT_TO_STRING_KV("ObITable", ObITable, KP(this), K_(timestamp), K_(active_trx_count), K_(state),
      K_(max_schema_version), K_(write_ref_cnt), K_(local_allocator), K_(with_accurate_log_ts_range), K_(frozen),
      K_(frozen_log_applied), K_(mark_finish), K_(pending_cb_cnt), K_(pending_lob_cnt), K_(pending_batch_commit_cnt),
      K_(pending_elr_cnt));

private:
  static const int64_t OB_EMPTY_MEMSTORE_MAX_SIZE = 10L << 20;  // 10MB
  int set_(const storage::ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_len,
      const common::ObIArray<share::schema::ObColDesc>& columns, const storage::ObStoreRow& new_row,
      const storage::ObStoreRow* old_row,  // old_row can be NULL, which means don't generate full log
      const common::ObIArray<int64_t>* update_idx);
  int m_clone_row_data(ObIMemtableCtx& ctx, ObRowData& src_row, ObRowData& dst_row);
  int m_clone_row_data(ObIMemtableCtx& ctx, ObMemtableCompactWriter& ccw, ObRowData& row);
  int record_rowkey_(
      ObMemtableCompactWriter& col_ccw, const ObMemtableKey& mtk, const ObIArray<share::schema::ObColDesc>& columns);
  int m_column_compact(const bool need_explict_record_rowkey, const ObMemtableKey& mtk,
      const common::ObIArray<share::schema::ObColDesc>& columns, const ObIArray<int64_t>* update_idx,
      const storage::ObStoreRow& row, ObMemtableCompactWriter& col_ccw, bool& is_lob_row);
  int m_column_compact(ObMemtableCompactWriter& col_ccw, const ObMemtableKey& mtk,
      const common::ObIArray<share::schema::ObColDesc>& columns);
  int m_replay_row(const storage::ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
      const char* data, const int64_t data_len, const storage::ObRowDml dml_type, const uint32_t modify_count,
      const uint32_t acc_checksum, const int64_t version, const int32_t sql_no, const int32_t flag,
      const int64_t log_timestamp);
  int m_prepare_kv(const storage::ObStoreCtx& ctx, const ObMemtableKey* key, ObMemtableKey* stored_key,
      ObMvccRow*& value, RowHeaderGetter& getter, const bool is_replay,
      const ObIArray<share::schema::ObColDesc>& columns, bool& is_new_add, bool& is_new_locked);
  int m_lock(const storage::ObStoreCtx& ctx, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, const ObMemtableKey* key, ObMemtableKey* stored_key,
      ObMvccRow*& value, bool& is_new_locked);

  int lock_row_on_frozen_stores(const storage::ObStoreCtx& ctx, const ObMemtableKey* key, ObMvccRow* value,
      const ObIArray<share::schema::ObColDesc>& columns);

  common::ObStoreRange& m_get_real_range_(
      common::ObStoreRange& real_range, const common::ObStoreRange& range, const bool is_reverse);
  void get_begin(const storage::ObStoreCtx& ctx);
  void get_end(const storage::ObStoreCtx& ctx, int ret);
  void scan_begin(const storage::ObStoreCtx& ctx);
  void scan_end(const storage::ObStoreCtx& ctx, int ret);
  void set_begin(const storage::ObStoreCtx& ctx);
  void set_end(const storage::ObStoreCtx& ctx, int ret);
  bool cluster_version_before_2100_() const
  {
    return GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2100;
  }

  int check_standby_cluster_schema_condition_(
      const storage::ObStoreCtx& ctx, const int64_t table_id, const int64_t table_version);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtable);
  bool is_inited_;
  ObMemstoreAllocator local_allocator_;
  ObMTKVBuilder kv_builder_;
  ObQueryEngine query_engine_;
  ObMvccEngine mvcc_engine_;
  storage::ObSavedStorageInfoV2 storage_info_;
  ObMtStat mt_stat_;
  int64_t max_schema_version_;  // to record the max schema version of all data
  int64_t max_trans_version_;
  int64_t active_trx_count_;  // deprecated
  int64_t pending_cb_cnt_;    // number of transactions have to sync log
  int64_t freeze_log_ts_;
  int64_t state_;
  int64_t timestamp_;
  bool read_barrier_ CACHE_ALIGNED;
  bool write_barrier_;
  int64_t write_ref_cnt_ CACHE_ALIGNED;
  int64_t row_relocate_count_;
  share::ObWorker::CompatMode mode_;
  int64_t minor_merged_time_;
  bool contain_hotspot_row_;
  int64_t pending_lob_cnt_;
  int64_t pending_batch_commit_cnt_;
  int64_t pending_elr_cnt_;
  struct {
    bool frozen_log_applied_ : 1;
    bool mark_finish_ : 1;
    bool frozen_ : 1;
    bool emergency_ : 1;
    bool with_accurate_log_ts_range_ : 1;
    bool is_split_ : 1;
    uint8_t reserved_ : 2;
  };
};

typedef ObMemtable ObMemStore;

/*
 * Print memtable statistics when receiving a signal.
 * For debug use.
 */
class ObMemtableStat {
public:
  ObMemtableStat();
  virtual ~ObMemtableStat();
  static ObMemtableStat& get_instance();

public:
  int register_memtable(ObMemtable* memtable);
  int unregister_memtable(ObMemtable* memtable);

public:
  int print_stat();

private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableStat);
  ObSpinLock lock_;
  ObArray<ObMemtable*> memtables_;
};

class RowHeaderGetter {
public:
  RowHeaderGetter()
      : ctx_(NULL), table_id_(common::OB_INVALID_ID), rowkey_(NULL), columns_(NULL), modify_count_(0), acc_checksum_(0)
  {}
  ~RowHeaderGetter()
  {}
  uint32_t get_modify_count() const
  {
    return modify_count_;
  }
  uint32_t get_acc_checksum() const
  {
    return acc_checksum_;
  }
  int get();

private:
  storage::ObStoreCtx* ctx_;
  uint64_t table_id_;
  common::ObStoreRowkey* rowkey_;
  common::ObIArray<share::schema::ObColDesc>* columns_;

private:
  uint32_t modify_count_;
  uint32_t acc_checksum_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif  // OCEANBASE_MEMTABLE_OB_MEMTABLE_
