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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_STORAGE
#define OCEANBASE_STORAGE_OB_PARTITION_STORAGE

#include "ob_partition_store.h"
#include "ob_relative_table.h"

namespace oceanbase {
namespace common {
class ObRowStore;
}
namespace share {
class ObPartitionReplica;
}
namespace blocksstable {
class ObBaseStorageLogger;
struct ObPartitionMeta;
struct ObStorageCacheSuite;
struct MacroBlockId;
class ObBuildIndexMacroBlockReader;
class ObBuildIndexMacroBlockWriter;
}  // namespace blocksstable
namespace memtable {
class ObMemtable;
}
namespace compaction {
class ObIStoreRowProcessor;
class ObSStableMergeEstimator;
class ObBuildIndexParam;
class ObBuildIndexContext;
}  // namespace compaction
namespace storageperf {
template <typename T>
class ObMultiBlockBench;
}
namespace storage {
class ObIPartitionGroupGuard;
class ObSSStore;
class ObBatch;
class ObPartitionMergeDag;
class ObMultipleMerge;
class ObSSTableCtx;
class ObSSTableSplitCtx;
class ObSSTableMergeCtx;
class ObTableScanIterIterator;
class ObPGMemtableMgr;
class ObPGStorage;
class ObTableScanIterator;

class ObStoreRowkeyHashFunc {
public:
  uint64_t operator()(const common::ObStoreRowkey& rowkey, const uint64_t hash)
  {
    return rowkey.murmurhash(hash);
  }
};

struct ObColumnChecksumEntry {
  ObColumnChecksumEntry() : checksum_(0), table_id_(0)
  {}
  ObColumnChecksumEntry(int64_t checksum, int64_t table_id) : checksum_(checksum), table_id_(table_id)
  {}
  int64_t checksum_;
  int64_t table_id_;
};

class ObStorageWriterGuard {
public:
  int refresh_and_protect_table(ObRelativeTable& relative_table);
  int refresh_and_protect_pg_memtable(ObPGStorage& pg_storage, ObTablesHandle& tables_handle);

  // called when writing to a specific partition
  ObStorageWriterGuard(
      ObPartitionStore& store, const ObStoreCtx& store_ctx, const bool need_control_mem, const bool is_replay = false)
      : need_control_mem_(need_control_mem),
        is_replay_(is_replay),
        store_(&store),
        store_ctx_(store_ctx),
        memtable_(NULL),
        pg_memtable_mgr_(NULL),
        retry_count_(0),
        last_ts_(0)
  {
    get_writing_throttling_sleep_interval() = 0;
  }
  // called when replay clog of pg
  ObStorageWriterGuard(const ObStoreCtx& store_ctx, ObPGMemtableMgr* pg_memtable_mgr, const bool need_control_mem,
      const bool is_replay = false)
      : need_control_mem_(need_control_mem),
        is_replay_(is_replay),
        store_(NULL),
        store_ctx_(store_ctx),
        memtable_(NULL),
        pg_memtable_mgr_(pg_memtable_mgr),
        retry_count_(0),
        last_ts_(0)
  {
    get_writing_throttling_sleep_interval() = 0;
  }
  ~ObStorageWriterGuard();
  ObStorageWriterGuard(const ObStorageWriterGuard&) = delete;
  ObStorageWriterGuard& operator=(const ObStorageWriterGuard&) = delete;

private:
  bool need_to_refresh_table(ObTablesHandle& tables_handle);
  bool check_if_need_log();
  void reset();

private:
  static const int64_t LOG_INTERVAL_US = 30 * 1000 * 1000;
  static const int64_t GET_TS_INTERVAL = 10 * 1000;

private:
  const bool need_control_mem_;
  const bool is_replay_;
  ObPartitionStore* store_;
  const ObStoreCtx& store_ctx_;
  memtable::ObMemtable* memtable_;
  ObPGMemtableMgr* pg_memtable_mgr_;
  int64_t retry_count_;
  int64_t last_ts_;
};

struct ObPartitionPrefixAccessStat {
  struct AccessStat {
    AccessStat()
    {
      reset();
    }
    ~AccessStat() = default;
    void reset()
    {
      memset(this, 0x00, sizeof(AccessStat));
    }
    bool is_valid() const;
    AccessStat& operator=(const AccessStat& other)
    {
      if (this != &other) {
        MEMCPY(this, &other, sizeof(AccessStat));
      }
      return *this;
    }
    TO_STRING_KV(K_(bf_filter_cnt), K_(bf_access_cnt), K_(empty_read_cnt));
    int64_t bf_filter_cnt_;
    int64_t bf_access_cnt_;
    int64_t empty_read_cnt_;
  };
  ObPartitionPrefixAccessStat()
  {
    reset();
  }
  ~ObPartitionPrefixAccessStat() = default;
  ObPartitionPrefixAccessStat& operator=(const ObPartitionPrefixAccessStat& other);
  void reset()
  {
    memset(this, 0x00, sizeof(ObPartitionPrefixAccessStat));
  }
  int add_stat(const ObTableAccessStat& stat);
  int add_stat(const ObTableScanStatistic& stat);
  int get_optimal_prefix(int64_t& prefix);
  int64_t to_string(char* buf, const int64_t buf_len) const;
  const static int64_t MAX_ROWKEY_PREFIX_NUM = 7;  // avoid consuming too much space for statistics, only count first 7
  AccessStat rowkey_prefix_[MAX_ROWKEY_PREFIX_NUM + 1];
};

class ObPartitionStorage : public ObIPartitionStorage {
  template <typename T>
  friend class oceanbase::storageperf::ObMultiBlockBench;
  friend class ObPGStorage;

public:
  ObPartitionStorage();
  virtual ~ObPartitionStorage();

  inline virtual const share::schema::ObMultiVersionSchemaService* get_schema_service() const override
  {
    return schema_service_;
  }
  virtual int init(const common::ObPartitionKey& pkey, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service, transaction::ObTransService* txs,
      ObPGMemtableMgr& pg_memtable_mgr) override;

  virtual void destroy() override;
  virtual const common::ObPartitionKey& get_partition_key() const override
  {
    return pkey_;
  }
  bool is_inited() const;

  TO_STRING_KV(K_(pkey), K_(store));
  //
  // scan table partition
  //
  // @param ctx [in] transaction context
  // @param param [in] query param
  // @param result [out] iterator to get the result set
  //
  // @return result iterator
  //
  virtual int table_scan(
      ObTableScanParam& param, const int64_t data_max_schema_version, common::ObNewRowIterator*& result) override;
  virtual int table_scan(
      ObTableScanParam& param, const int64_t data_max_schema_version, common::ObNewIterIterator*& result) override;
  virtual int join_mv_scan(ObTableScanParam& left_param, ObTableScanParam& right_param,
      const int64_t left_data_max_schema_version, const int64_t right_data_max_schema_version,
      ObIPartitionStorage& right_storage, common::ObNewRowIterator*& result) override;
  virtual int revert_scan_iter(common::ObNewRowIterator* iter) override;
  //
  // delete rows
  //     delete table rows and index rows
  //
  // @param trans_desc [in] transaction handle
  // @param timeout [in] process timeout
  // @param table_id [in] table
  // @param index_included [in] need to delete index too
  // @param column_ids [in] all column referenced, rowkey first
  // @param row_iter [in] primary keys to be deleted
  // @param affected_rows [out]
  //
  // @retval OB_TRANSACTION_SET_VIOLATION
  // @retval OB_SNAPSHOT_DISCARDED
  // @retval OB_TRANS_NOT_FOUND
  // @retval OB_TRANS_ROLLBACKED
  // @retval OB_TRANS_IS_READONLY
  // @retval OB_ERR_EXCLUSIVE_LOCK_CONFLICT
  //
  virtual int delete_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter,
      int64_t& affected_rows) override;

  virtual int delete_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row) override;

  virtual int put_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter,
      int64_t& affected_rows) override;

  //
  // insert rows
  //     insert table rows and index rows
  //
  // @param trans_desc [in] transaction handle
  // @param timeout [in] process timeout
  // @param table_id [in] table
  // @param column_ids [in] insert columns
  // @param row_iter [in] insert values
  // @param affected_rows [out]
  //
  // @retval OB_TRANS_NOT_FOUND
  // @retval OB_TRANS_ROLLBACKED
  // @retval OB_TRANS_IS_READONLY
  // @retval OB_ERR_EXCLUSIVE_LOCK_CONFLICT
  // @retval OB_ERR_PRIMARY_KEY_DUPLICATE
  //
  virtual int insert_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter,
      int64_t& affected_rows) override;
  //
  // insert row
  //     insert table row or return conflict row(s)
  //
  // @param trans_desc [in] transaction handle
  // @param timeout [in] process timeout
  // @param column_ids [in] insert columns
  // @param duplicated_column_ids [in] output columns when conflict met
  // @param row [in] row to be inserted
  // @param flag [in] return all conflict rows or the first one
  // @param affected_rows [out] successfully insert row number
  // @param duplicated_rows [out] the iterator of the rowkey(s) of conflict row(s)
  //
  virtual int insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row);

  virtual int insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& duplicated_column_ids,
      const common::ObNewRow& row, const ObInsertFlag flag, int64_t& affected_rows,
      common::ObNewRowIterator*& duplicated_rows) override;
  // check whether row has conflict in storage
  // in_column_ids describe columns of the row, begin with rowey, must include local unique index
  // out_column_ids describe column of conflict row
  // check_row_iter is the iterator of rows that will be checked
  // dup_row_iters are iterators of conflict rows, the number of iterators is same with number of checked rows
  virtual int fetch_conflict_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& in_column_ids, const common::ObIArray<uint64_t>& out_column_ids,
      common::ObNewRowIterator& check_row_iter, common::ObIArray<common::ObNewRowIterator*>& dup_row_iters) override;
  virtual int revert_insert_iter(common::ObNewRowIterator* iter) override;
  //
  // update rows
  //     update table rows and index rows
  //
  // @param trans_desc [in] transaction handle
  // @param timeout [in] process timeout
  // @param table_id [in] table
  // @param index_included [in] if index need to be updated
  // @param column_ids [in] all columns related
  // @param column_ids [in] updated columns
  // @param row_iter [in] odd rows are old and even rows are new ones
  // @param affected_rows [out]
  //
  // @retval OB_TRANSACTION_SET_VIOLATION
  // @retval OB_SNAPSHOT_DISCARDED
  // @retval OB_TRANS_NOT_FOUND
  // @retval OB_TRANS_ROLLBACKED
  // @retval OB_TRANS_IS_READONLY
  // @retval OB_ERR_EXCLUSIVE_LOCK_CONFLICT
  // @retval OB_ERR_PRIMARY_KEY_DUPLICATE
  //
  virtual int update_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& updated_column_ids,
      common::ObNewRowIterator* row_iter, int64_t& affected_rows) override;
  virtual int update_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& updated_column_ids,
      const common::ObNewRow& old_row, const common::ObNewRow& new_row) override;

  //
  // lock rows
  //     lock table rows
  //
  // @param trans_desc [in] transaction handle
  // @param timeout [in] process timeout
  // @param table_id [in] table id
  // @param row_iter [in] rowkey iterator
  // @param lock_flag [in] lock flags: LF_WRITE or LF_NONE
  // @param affected_rows [out]
  //
  // @retval OB_TRANSACTION_SET_VIOLATION
  // @retval OB_SNAPSHOT_DISCARDED
  // @retval OB_TRANS_NOT_FOUND
  // @retval OB_TRANS_ROLLBACKED
  // @retval OB_TRANS_IS_READONLY
  // @retval OB_ERR_EXCLUSIVE_LOCK_CONFLICT
  //
  virtual int lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      common::ObNewRowIterator* row_iter, ObLockFlag lock_flag, int64_t& affected_rows) override;
  virtual int lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      const common::ObNewRow& row, ObLockFlag lock_flag) override;

  virtual int get_concurrent_cnt(uint64_t table_id, int64_t schema_version, int64_t& concurrent_cnt) override;

  virtual int get_batch_rows(const ObTableScanParam& param, const storage::ObBatch& batch, int64_t& logical_row_count,
      int64_t& physical_row_count, common::ObIArray<common::ObEstRowCountRecord>& est_records) override;

  virtual int get_table_stat(const uint64_t table_id, common::ObTableStat& stat) override;

  virtual int lock(const ObStoreCtx& ctx) override;

  virtual void set_merge_status(bool merge_success);
  virtual bool can_schedule_merge();

  // write ssstore objects @version tree to data file, used by write_check_point
  virtual int serialize(ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size);
  // read ssstore objects from data file to construct partition storage's version tree.
  virtual int deserialize(const ObReplicaType replica_type, const char* buf, const int64_t buf_len,
      ObIPartitionGroup* pg, int64_t& pos, bool& is_old_meta, ObPartitionStoreMeta& old_meta);

  virtual bool has_memstore();

  // freeze actions
  virtual int get_replayed_table_version(int64_t& table_version);
  virtual int get_partition_ss_store_info(common::ObArray<PartitionSSStoreInfo>& partition_ss_store_info_list);
  virtual int get_all_tables(ObTablesHandle& tables_handle);
  virtual int retire_warmup_store(const bool is_disk_full);
  virtual int halt_prewarm();

  virtual common::ObReplicaType get_replica_type()
  {
    return store_.get_replica_type();
  }
  virtual int query_range_to_macros(common::ObIAllocator& allocator,
      const common::ObIArray<common::ObStoreRange>& ranges, const int64_t type, uint64_t* macros_count,
      const int64_t* total_task_count, common::ObIArray<common::ObStoreRange>* splitted_ranges,
      common::ObIArray<int64_t>* split_index);
  virtual int get_multi_ranges_cost(const common::ObIArray<common::ObStoreRange>& ranges, int64_t& total_size);
  virtual int split_multi_ranges(const common::ObIArray<common::ObStoreRange>& ranges,
      const int64_t expected_task_count, common::ObIAllocator& allocator,
      common::ObArrayArray<common::ObStoreRange>& multi_range_split_array);
  virtual int append_local_sort_data(const share::ObBuildIndexAppendLocalDataParam& param,
      const common::ObPGKey& pg_key, const blocksstable::ObStorageFileHandle& file_handle,
      common::ObNewRowIterator& iter) override;

  int get_schemas_to_split(storage::ObSSTableSplitCtx& ctx);
  int get_schemas_to_merge(storage::ObSSTableMergeCtx& ctx);
  int check_is_schema_changed(const int64_t column_checksum_method,
      const share::schema::ObTableSchema& base_table_schema, const share::schema::ObTableSchema& main_table_schema,
      bool& is_schema_changed, bool& is_column_changed, bool& is_progressive_merge_num_changed);
  int cal_major_merge_param(ObSSTableMergeCtx& ctx);
  int cal_minor_merge_param(ObSSTableMergeCtx& ctx);
  int build_merge_ctx(storage::ObSSTableMergeCtx& ctx);
  int get_concurrent_cnt(ObTablesHandle& tables_handle, const share::schema::ObTableSchema& table_schema,
      const ObMergeType& merge_type, int64_t& concurrent_cnt);

  int init_split_context(storage::ObSSTableSplitCtx& ctx);
  int init_merge_context(storage::ObSSTableMergeCtx& ctx);

  // TODO(): following methods need to be moved to merge_task.cpp later
  static void check_data_checksum(const common::ObReplicaType& replica_type, const ObPartitionKey& pkey,
      const share::schema::ObTableSchema& schema, const ObSSTable& data_sstable,
      common::ObIArray<storage::ObITable*>& base_tables, ObIPartitionReport& report);
  static void dump2text(const share::schema::ObTableSchema& schema, common::ObIArray<storage::ObITable*>& base_tables,
      const ObPartitionKey& pkey);
  static int update_estimator(const share::schema::ObTableSchema* base_schema, const bool is_full,
      const ObIArray<ObColumnStat*>& column_stats, ObSSTable* sstable, const common::ObPartitionKey& pkey);
  int create_partition_store(const common::ObReplicaType& replica_type, const int64_t multi_version_start,
      const uint64_t data_table_id, const int64_t create_schema_version, const int64_t create_timestamp,
      ObIPartitionGroup* pg, ObTablesHandle& sstables_handle);
  ObPartitionStore& get_partition_store()
  {
    return store_;
  }
  int do_warm_up_request(const ObIWarmUpRequest* request);
  int check_index_need_build(const share::schema::ObTableSchema& index_schema, bool& need_build);
  int get_build_index_context(const compaction::ObBuildIndexParam& param, compaction::ObBuildIndexContext& context);

  int local_sort_index_by_range(
      const int64_t idx, const compaction::ObBuildIndexParam& param, const compaction::ObBuildIndexContext& context);

  int get_build_index_param(const uint64_t index_id, const int64_t schema_version, ObIPartitionReport* report,
      compaction::ObBuildIndexParam& param) override;
  int save_split_state(const int64_t state, const bool write_slog);
  int get_split_state(int64_t& state) const;
  int save_split_info(const ObPartitionSplitInfo& split_info, const bool write_slog);
  int check_table_continuity(const uint64_t table_id, const int64_t version, bool& is_continual);
  template <typename T>
  static int get_merge_opt(const int64_t merge_version, const int64_t storage_version, const int64_t work_version,
      const int64_t born_version, const T& old_val, const T& new_val, T& opt);
  int update_multi_version_start(const int64_t multi_version_start) override;
  int validate_sstables(const ObIArray<share::schema::ObIndexTableStat>& index_stats, bool& is_all_checked);
  bool need_check_index_(const uint64_t index_id, const ObIArray<share::schema::ObIndexTableStat>& index_stats);
  static int generate_index_output_param(const share::schema::ObTableSchema& data_table_schema,
      const share::schema::ObTableSchema& index_schema, common::ObArray<share::schema::ObColDesc>& col_ids,
      common::ObArray<share::schema::ObColDesc>& org_col_ids, common::ObArray<int32_t>& output_projector,
      int64_t& unique_key_cnt);
  int fill_checksum(const uint64_t index_id, const int sstable_type, share::ObSSTableChecksumItem& checksum_item);
  ObPartitionPrefixAccessStat& get_prefix_access_stat()
  {
    return prefix_access_stat_;
  }
  int feedback_scan_access_stat(const ObTableScanParam& param);

  int erase_stat_cache();

public:
  typedef common::hash::ObHashMap<int64_t, ObColumnChecksumEntry, common::hash::NoPthreadDefendMode> ColumnChecksumMap;

private:
  struct RowReshape {
    RowReshape()
        : row_reshape_cells_(NULL),
          char_only_(false),
          binary_buffer_len_(0),
          binary_buffer_ptr_(NULL),
          binary_len_array_()
    {}
    common::ObObj* row_reshape_cells_;
    bool char_only_;
    int64_t binary_buffer_len_;
    char* binary_buffer_ptr_;
    // pair: binary column idx in row, binary column len
    common::ObSEArray<std::pair<int32_t, int32_t>, common::OB_ROW_MAX_COLUMNS_COUNT> binary_len_array_;
  };

  enum ChangeType {
    NO_CHANGE,
    ROWKEY_CHANGE,
    ND_ROWKEY_CHANGE,  // null dependent rowkey change
    OTHER_CHANGE,
  };

  struct ObDMLRunningCtx {
  public:
    ObDMLRunningCtx(const ObStoreCtx& store_ctx, const ObDMLBaseParam& dml_param, common::ObIAllocator& allocator,
        const ObRowDml dml_type)
        : store_ctx_(store_ctx),
          dml_param_(dml_param),
          allocator_(allocator),
          dml_type_(dml_type),
          relative_tables_(allocator),
          col_map_(nullptr),
          col_descs_(nullptr),
          idx_col_descs_(),
          tbl_row_(),
          idx_row_(NULL),
          is_inited_(false)
    {}
    ~ObDMLRunningCtx()
    {
      free_work_members();
    }

    int init(const common::ObIArray<uint64_t>* column_ids, const common::ObIArray<uint64_t>* upd_col_ids,
        const bool use_table_param, share::schema::ObMultiVersionSchemaService* schema_service,
        ObPartitionStore& store);
    static int prepare_column_desc(
        const common::ObIArray<uint64_t>& column_ids, const ObRelativeTable& table, ObColDescIArray& col_descs);

  private:
    void free_work_members();
    int prepare_index_row();
    int prepare_column_info(const common::ObIArray<uint64_t>& column_ids);

  public:
    const ObStoreCtx& store_ctx_;
    const ObDMLBaseParam& dml_param_;
    common::ObIAllocator& allocator_;
    const ObRowDml dml_type_;
    ObRelativeTables relative_tables_;
    const share::schema::ColumnMap* col_map_;
    const ObColDescIArray* col_descs_;
    ObColDescArray idx_col_descs_;
    ObStoreRow tbl_row_;
    ObStoreRow* idx_row_;  // not a must, allocate dynamically

  private:
    bool is_inited_;
  };

  static const int32_t LOCK_WAIT_INTERVAL = 5;  // 5us

private:
  int write_index_row(ObRelativeTable& relative_table, const ObStoreCtx& ctx, const ObColDescIArray& idx_columns,
      ObStoreRow& index_row);
  int check_other_columns_in_column_ids(const ObRelativeTables& rel_schema, const share::schema::ColumnMap* col_map,
      const common::ObIArray<uint64_t>& column_ids, const ObRowDml dml_type, const ChangeType change_type,
      const bool is_total_quantity_log);
  int construct_column_ids_map(const common::ObIArray<uint64_t>& column_ids, const int64_t total_column_count,
      common::ObIAllocator& work_allocator, share::schema::ColumnMap*& col_map);
  void deconstruct_column_ids_map(common::ObIAllocator& work_allocator, share::schema::ColumnMap*& col_map);
  int check_column_ids_valid(common::ObIAllocator& work_allocator, const ObRelativeTables& relative_tables,
      const share::schema::ColumnMap* col_map, const common::ObIArray<uint64_t>& column_ids, const ObRowDml dml_type,
      const common::ObIArray<uint64_t>& upd_column_ids, const ChangeType change_type, const bool is_total_quantity_log);
  int insert_table_row(
      ObDMLRunningCtx& run_ctx, ObRelativeTable& relative_table, const ObColDescIArray& col_descs, ObStoreRow& row);
  int insert_table_rows(ObDMLRunningCtx& run_ctx, ObRelativeTable& relative_table, const ObColDescIArray& col_descs,
      ObRowsInfo& rows_info);
  int insert_index_rows(ObDMLRunningCtx& run_ctx, ObStoreRow* rows, int64_t row_count);
  int direct_insert_row_and_index(ObDMLRunningCtx& run_ctx, const ObStoreRow& tbl_row);
  int get_column_index(const ObColDescIArray& tbl_col_desc, const ObColDescIArray& idx_col_desc,
      common::ObIArray<int32_t>& col_idx_array);
  int convert_row_to_rowkey(common::ObNewRowIterator& iter, GetRowkeyArray& rowkeys);
  int get_conflict_row(ObDMLRunningCtx& run_ctx, const ObTableAccessParam& access_param,
      ObTableAccessContext& access_ctx, ObRelativeTable& relative_table, const common::ObStoreRowkey& rowkey,
      common::ObNewRowIterator*& duplicated_rows);
  int get_index_conflict_row(ObDMLRunningCtx& run_ctx, const ObTableAccessParam& table_access_param,
      ObTableAccessContext& table_access_ctx, ObRelativeTable& relative_table, bool need_index_back,
      const common::ObNewRow& row, common::ObNewRowIterator*& duplicated_rows);
  int multi_get_rows(const ObStoreCtx& store_ctx, const ObTableAccessParam& access_param,
      ObTableAccessContext& access_ctx, ObRelativeTable& relative_table, const GetRowkeyArray& rowkeys,
      common::ObNewRowIterator*& duplicated_rows);
  int get_conflict_rows(ObDMLRunningCtx& run_ctx, const ObInsertFlag flag,
      const common::ObIArray<uint64_t>& dup_col_ids, const common::ObNewRow& row,
      common::ObNewRowIterator*& duplicated_rows);
  int init_dml_access_ctx(ObDMLRunningCtx& run_ctx, common::ObArenaAllocator& allocator,
      blocksstable::ObBlockCacheWorkingSet& block_cache_ws, ObTableAccessContext& table_access_ctx);
  int get_change_type(
      const common::ObIArray<uint64_t>& update_ids, const ObRelativeTable& table, ChangeType& change_type);
  int check_rowkey_change(const common::ObIArray<uint64_t>& update_ids, const ObRelativeTables& relative_tables,
      common::ObIArray<ChangeType>& changes, bool& delay_new);
  int check_rowkey_value_change(const common::ObNewRow& old_row, const common::ObNewRow& new_row,
      const int64_t rowkey_len, bool& rowkey_change) const;
  int process_old_row(ObDMLRunningCtx& ctx, const bool data_tbl_rowkey_change,
      const common::ObIArray<ChangeType>& change_flags, const ObStoreRow& tbl_row);
  int process_row_of_data_table(ObDMLRunningCtx& run_ctx, const common::ObIArray<int64_t>& update_idx,
      const ObStoreRow& old_tbl_row, const ObStoreRow& new_tbl_row, const bool rowkey_change);
  int process_row_of_index_tables(
      ObDMLRunningCtx& run_ctx, const common::ObIArray<ChangeType>& change_flags, const ObStoreRow& new_tbl_row);
  int process_new_row(ObDMLRunningCtx& run_ctx, const common::ObIArray<ChangeType>& change_flags,
      const common::ObIArray<int64_t>& update_idx, const ObStoreRow& old_tbl_row, const ObStoreRow& new_tbl_row,
      const bool rowkey_change);
  int reshape_delete_row(
      ObDMLRunningCtx& run_ctx, RowReshape*& row_reshape, ObStoreRow& tbl_row, ObStoreRow& new_tbl_row);
  int delete_row(ObDMLRunningCtx& run_ctx, RowReshape*& row_reshape, const common::ObNewRow& row);
  int update_row(ObDMLRunningCtx& run_ctx, const common::ObIArray<ChangeType>& changes,
      const common::ObIArray<int64_t>& update_idx, const bool delay_new, RowReshape*& old_row_reshape_ins,
      RowReshape*& row_reshape_ins, ObStoreRow& old_tbl_row, ObStoreRow& new_tbl_row, ObRowStore* row_store,
      bool& duplicate);
  bool illegal_filter(const ObTableScanParam& param) const;

  // $lta_param && $lta_ctx are inited
  int join_mv_init_merge_param(const share::schema::ObTableSchema& ltable, const share::schema::ObTableSchema& rtable,
      const ObTableAccessParam& lta_param, const ObTableAccessContext& lta_ctx, share::schema::ObTableParam& rt_param,
      ObTableAccessParam& rta_param, ObTableAccessContext& rta_ctx, ObStoreCtx& rctx);

  int prepare_merge_mv_depend_sstable(const common::ObVersion& frozen_version,
      const share::schema::ObTableSchema* schema, const share::schema::ObTableSchema* dep_schema,
      ObTablesHandle& mv_dep_tables_handle);

  int estimate_row_count(const storage::ObTableScanParam& param, const storage::ObBatch& batch,
      const common::ObIArray<ObITable*>& stores, ObPartitionEst& cost_estimate,
      common::ObIArray<common::ObEstRowCountRecord>& est_records);
  // In sql static engine cell is projected to ObExpr directly in ObMultipleMerge.
  // We need to project back to ObNewRow when for row locking (lock_row() need ObNewRow).
  //
  // Why not project to ObNewRow directly in static engine for lock? Because the filter
  // need the ObExpr interface.
  int prepare_lock_row(const ObTableScanParam& scan_param, const common::ObNewRow& row);
  int lock_scan_rows(const ObStoreCtx& ctx, const ObTableScanParam& scan_param, ObTableScanIterator& iter);
  int lock_scan_rows(const ObStoreCtx& ctx, const ObTableScanParam& scan_param, ObTableScanIterIterator& iter);
  int64_t get_lock_wait_timeout(const int64_t abs_lock_timeout, const int64_t stmt_timeout) const;
  int get_next_row_from_iter(common::ObNewRowIterator* iter, ObStoreRow& store_row, const bool need_copy_cells);
  int construct_update_idx(const share::schema::ColumnMap* col_map, const common::ObIArray<uint64_t>& upd_col_ids,
      common::ObIArray<int64_t>& update_idx);
  int malloc_rows_reshape_if_need(common::ObIAllocator& work_allocator, const ObColDescIArray& col_descs,
      const int64_t row_count, const ObRelativeTable& table, const ObSQLMode sql_mode, RowReshape*& row_reshape_ins);
  int malloc_rows_reshape(common::ObIAllocator& work_allocator, const ObColDescIArray& col_descs,
      const int64_t row_count, const ObRelativeTable& table, RowReshape*& row_reshape_ins);
  void free_row_reshape(common::ObIAllocator& work_allocator, RowReshape*& row_reshape_ins, int64_t row_count);
  int need_reshape_table_row(const common::ObNewRow& row, RowReshape* row_reshape_ins, int64_t row_reshape_cells_count,
      ObSQLMode sql_mode, bool& need_reshape) const;
  int need_reshape_table_row(
      const common::ObNewRow& row, const int64_t column_cnt, ObSQLMode sql_mode, bool& need_reshape) const;
  int reshape_row(const common::ObNewRow& row, const int64_t column_cnt, RowReshape* row_reshape_ins,
      ObStoreRow& tbl_row, bool need_reshape, ObSQLMode sql_mode) const;
  int reshape_table_rows(const common::ObNewRow* rows, RowReshape* row_reshape_ins, int64_t row_reshape_cells_count,
      ObStoreRow* tbl_rows, int64_t row_count, ObSQLMode sql_mode) const;

  virtual int extract_rowkey(const ObRelativeTable& table, const common::ObStoreRowkey& rowkey, char* buffer,
      const int64_t buffer_len, const common::ObTimeZoneInfo* tz_info = NULL);

private:
  int get_depend_table_schema(const share::schema::ObTableSchema* table_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema*& dep_table_schema);
  int report_checksum(const uint64_t execution_id, const uint64_t task_id,
      const share::schema::ObTableSchema& index_schema, const int64_t checksum_method, int64_t* column_checkum);

  int get_build_index_stores(const share::schema::ObTenantSchema& tenant_schema, compaction::ObBuildIndexParam& param);
  int check_need_update_estimator(
      const share::schema::ObTableSchema& table_schema, int64_t data_version, int64_t& stat_sampling_ratio);
  int do_rowkey_exists(const ObStoreCtx& store_ctx, const common::ObIArray<ObITable*>& read_tables,
      const int64_t table_id, const common::ObStoreRowkey& rowkey, const common::ObQueryFlag& query_flag,
      const ObColDescIArray& col_descs, bool& exists);
  int rowkey_exists(ObRelativeTable& relative_table, const ObStoreCtx& store_ctx, const ObColDescIArray& col_descs,
      const common::ObNewRow& row, bool& exists);
  int do_rowkeys_prefix_exist(const common::ObIArray<ObITable*>& read_stores, ObRowsInfo& rows_info, bool& may_exist);
  int do_rowkeys_exists(const common::ObIArray<ObITable*>& read_stores, ObRowsInfo& rows_info, bool& exists);
  int rowkeys_exists(const ObStoreCtx& store_ctx, ObRelativeTable& relative_table, ObRowsInfo& rows_info, bool& exists);
  int write_row(ObRelativeTable& relative_table, const ObStoreCtx& store_ctx, const int64_t rowkey_len,
      const common::ObIArray<share::schema::ObColDesc>& col_descs, const storage::ObStoreRow& row);
  int write_row(ObRelativeTable& relative_table, const storage::ObStoreCtx& ctx, const int64_t rowkey_len,
      const common::ObIArray<share::schema::ObColDesc>& col_descs, const common::ObIArray<int64_t>& update_idx,
      const storage::ObStoreRow& old_row, const storage::ObStoreRow& new_row);
  int lock_row(ObRelativeTable& relative_table, const storage::ObStoreCtx& store_ctx,
      const common::ObIArray<share::schema::ObColDesc>& col_descs, const common::ObNewRow& row,
      const ObSQLMode sql_mode, ObIAllocator& allocator, RowReshape*& row_reshape_ins);
  int lock_row(ObRelativeTable& relative_table, const storage::ObStoreCtx& store_ctx,
      const common::ObIArray<share::schema::ObColDesc>& col_descs, const common::ObStoreRowkey& rowkey);
  int check_row_locked_by_myself(ObRelativeTable& relative_table, const storage::ObStoreCtx& store_ctx,
      const common::ObIArray<share::schema::ObColDesc>& col_descs, const common::ObStoreRowkey& rowkey, bool& locked);

  int insert_rows_(ObDMLRunningCtx& run_ctx, const common::ObNewRow* const rows, const int64_t row_count,
      ObRowsInfo& rows_info, storage::ObStoreRow* tbl_rows, RowReshape* row_reshape_ins, int64_t& afct_num,
      int64_t& dup_num);

  bool is_expired_version(
      const common::ObVersion& base_version, const common::ObVersion& version, const int64_t max_kept_number);

  int get_merge_level(const int64_t merge_version, const ObSSTableMergeCtx& ctx, ObMergeLevel& merge_level);

  int get_store_column_checksum_in_micro(
      const int64_t merge_version, const ObSSTableMergeCtx& ctx, bool& store_column_checksum);

  int get_local_index_column_ids_and_projector(const share::schema::ObTableSchema& table_schema,
      const share::schema::ObTableSchema& index_schema, ObArray<share::schema::ObColDesc>& org_col_ids,
      ObArray<share::schema::ObColDesc>& col_ids, ObArray<int32_t>& output_projector);

  int init_partition_meta(const common::ObReplicaType& replica_type, int64_t data_version,
      const int64_t multi_version_start, const uint64_t data_table_id, const int64_t create_schema_version,
      const int64_t create_timestamp, const int64_t snapshot_version, ObPGPartitionStoreMeta& partition_meta);
  int replay_old_ssstore(ObSSStore& store, int64_t& last_publish_version, ObArray<ObITable::TableKey>& add_tables);
  int deep_copy_build_index_schemas(const share::schema::ObTableSchema* data_table_schema,
      const share::schema::ObTableSchema* index_schema, const share::schema::ObTableSchema* dep_table_schema,
      compaction::ObBuildIndexParam& param);
  int validate_sstable(const int64_t row_count, const ObSSTable* sstable, ColumnChecksumMap& cc_map);
  void handle_error_index_table(
      const ObSSTable& index_table, const ObIArray<share::schema::ObIndexTableStat>& index_stats, int& result);
  int try_update_report_status(const common::ObVersion& version, bool& finished, bool& need_report);
  int check_schema_version_for_bounded_staleness_read_(
      const int64_t table_version_for_read, const int64_t data_max_schema_version, const uint64_t table_id);
  int lock_rows_(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      const common::ObNewRow& row, ObLockFlag lock_flag, RowReshape*& row_reshape);
  int lock_rows_(
      const ObStoreCtx& ctx, const ObTableScanParam& scan_param, const common::ObNewRow& row, RowReshape*& row_reshape);
  int dump_error_info(ObSSTable& main_sstable, ObSSTable& index_sstable);
  // disallow copy;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionStorage);

protected:
  // data members
  static const int64_t DELAY_SCHEDULE_TIME_UNIT = 1000 * 1000 * 1;  // 1s
  static const int64_t MAGIC_NUM_BEFORE_1461 = -0xABCD;
  static const int64_t MAGIC_NUM_1_4_61 = -0xABCE;
  uint64_t cluster_version_;
  common::ObPartitionKey pkey_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObIPartitionComponentFactory* cp_fty_;
  transaction::ObTransService* txs_;
  ObPGMemtableMgr* pg_memtable_mgr_;
  bool is_inited_;
  bool merge_successed_;
  int64_t merge_timestamp_;
  int64_t merge_failed_cnt_;
  ObPartitionStore store_;
  common::ObReplicaType replay_replica_type_;  // only for compatibility of 1.4, used in replaying slog
  ObPartitionPrefixAccessStat prefix_access_stat_;
};

inline int64_t ObPartitionStorage::get_lock_wait_timeout(
    const int64_t abs_lock_timeout, const int64_t stmt_timeout) const
{
  return (abs_lock_timeout < 0 ? stmt_timeout : (abs_lock_timeout > stmt_timeout ? stmt_timeout : abs_lock_timeout));
}

}  // namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PARTITION_STORAGE
