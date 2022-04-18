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

#ifndef OCEANBASE_STORAGE_OB_I_PARTITION_STORAGE_
#define OCEANBASE_STORAGE_OB_I_PARTITION_STORAGE_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array_array.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/time/ob_time_utility.h"
#include "common/row/ob_row_iterator.h"
#include "common/ob_partition_key.h"
#include "common/ob_range.h"
#include "share/ob_i_data_access_service.h"
#include "storage/ob_i_store.h"

namespace oceanbase {
namespace common {
class ObBaseStorageInfo;
class MockObNewRowIterator;
class ObColumnStat;
class ObTableStat;
}  // namespace common
namespace share {
class ObBuildIndexAppendLocalDataParam;
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
}  // namespace schema
}  // namespace share
namespace blocksstable {
class ObStorageCacheSuite;
class ObMacroBlockMarkerHelper;
class ObBuildIndexMacroBlockReader;
class ObBuildIndexMacroBlockWriter;
class MacroBlockId;
class ObStorageFileHandle;
}  // namespace blocksstable
namespace transaction {
class ObTransService;
}
namespace compaction {
class ObIStoreRowProcessor;
class ObSStableMergeEstimator;
struct ObBuildIndexParam;
struct ObBuildIndexContext;
class ObMergeIndexInfo;
}  // namespace compaction
namespace storage {
class ObStoreHandle;
class ObBatch;
class ObPartitionMergeDag;
class ObPartitionStorage;
class ObStoreRowComparer;
template <typename T, typename Compare>
class ObExternalSort;
class ObSSTableMergeContext;
class ObPGMemtableMgr;
class ObIPartitionReport;
class ObTableScanParam;
class ObStoreCtx;
class ObIWarmUpRequest;
class ObTablesHandle;
class ObDMLBaseParam;

enum ObInsertFlag {
  INSERT_RETURN_ALL_DUP = 0,
  INSERT_RETURN_ONE_DUP = 1,
};

enum FreezeType {
  INVALID = 0,
  AUTO_TRIG = 1,
  FORCE_MINOR = 2,
  FORCE_MAJOR = 3,
};

enum ObPartitionStorageStatus { PARTITION_INVALID = 0, PARTITION_FREEZING, PARTITION_FROZEN, PARTITION_NORMAL };

class ObIPartitionComponentFactory;

struct PartitionSSStoreInfo {
  common::ObVersion version_;
  int64_t macro_block_count_;
  int64_t use_old_macro_block_count_;
  bool is_merged_;
  bool is_modified_;
  int64_t rewrite_macro_old_micro_block_count_;
  int64_t rewrite_macro_total_micro_block_count_;
  PartitionSSStoreInfo()
      : version_(),
        macro_block_count_(0),
        use_old_macro_block_count_(0),
        is_merged_(false),
        is_modified_(false),
        rewrite_macro_old_micro_block_count_(0),
        rewrite_macro_total_micro_block_count_(0)
  {}
  void reset()
  {
    version_.reset();
    macro_block_count_ = 0;
    use_old_macro_block_count_ = 0;
    is_merged_ = false;
    is_modified_ = false;
    rewrite_macro_old_micro_block_count_ = 0;
    rewrite_macro_total_micro_block_count_ = 0;
  }
  TO_STRING_KV(K_(version), K_(macro_block_count), K_(use_old_macro_block_count), K_(is_merged), K_(is_modified),
      K_(rewrite_macro_old_micro_block_count), K_(rewrite_macro_total_micro_block_count));
};

class ObIPartitionStorage {
public:
  ObIPartitionStorage()
  {}
  virtual ~ObIPartitionStorage()
  {}

  virtual int init(const common::ObPartitionKey& pkey, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service, transaction::ObTransService* txs,
      ObPGMemtableMgr& pg_memtable_mgr) = 0;

  virtual void destroy() = 0;
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
      ObTableScanParam& param, const int64_t data_max_schema_version, common::ObNewRowIterator*& result) = 0;

  virtual int table_scan(
      ObTableScanParam& param, const int64_t data_max_schema_version, common::ObNewIterIterator*& result) = 0;

  virtual int join_mv_scan(ObTableScanParam& left_param, ObTableScanParam& right_param,
      const int64_t left_data_max_schema_version, const int64_t right_data_max_schema_version,
      ObIPartitionStorage& right_storage, common::ObNewRowIterator*& result) = 0;
  //
  // release scan iterator
  //
  // @param iter [in] iterator to be reverted
  //
  virtual int revert_scan_iter(common::ObNewRowIterator* iter) = 0;
  //
  // delete rows
  //     delete table rows and index rows
  //
  // @param trans_desc [in] transaction handle
  // @param timeout [in] process timeout
  // @param table_id [in] table
  // @param index_included [in] need to delete index too
  // @param column_ids [in] all column referenced.rowkey first
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
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows) = 0;

  virtual int delete_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObNewRow& row) = 0;

  virtual int put_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows) = 0;

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
      const common::ObIArray<uint64_t>& column_ids, common::ObNewRowIterator* row_iter, int64_t& affected_rows) = 0;

  /// @param duplicated_rowkeys [out] when failed with duplicated key error, return all the existing rows' primary keys
  //
  // insert row
  //     insert table row or return conflict row(s)
  //
  // @param trans_desc [in] transaction handle
  // @param timeout [in] process timeout
  // @param column_ids [in] insert columns
  // @param duplicated_column_ids [in] output columns when confliction met
  // @param row [in] row to be inserted
  // @param flag [in] return all conflict rows or the first one
  // @param affected_rows [out] successfully insert row number
  // @param duplicated_rows [out] the iterator of the rowkey(s) of conflict row(s)
  //
  virtual int insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& duplicated_column_ids,
      const common::ObNewRow& row, const ObInsertFlag flag, int64_t& affected_rows,
      common::ObNewRowIterator*& duplicated_rows) = 0;
  // check whether row has conflict in storage
  // in_column_ids describe columns of the row, begin with rowey, must include local unique index
  // out_column_ids describe column of conflict row
  // check_row_iter is the iterator of rows that will be checked
  // dup_row_iters are iterators of conflict rows, the number of iterators is same with number of checked rows
  virtual int fetch_conflict_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& in_column_ids, const common::ObIArray<uint64_t>& out_column_ids,
      common::ObNewRowIterator& check_row_iter, common::ObIArray<common::ObNewRowIterator*>& dup_row_iters) = 0;
  virtual int revert_insert_iter(common::ObNewRowIterator* iter) = 0;
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
      common::ObNewRowIterator* row_iter, int64_t& affected_rows) = 0;
  virtual int update_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const common::ObIArray<uint64_t>& column_ids, const common::ObIArray<uint64_t>& updated_column_ids,
      const common::ObNewRow& old_row, const common::ObNewRow& new_row) = 0;

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
      common::ObNewRowIterator* row_iter, ObLockFlag lock_flag, int64_t& affected_rows) = 0;
  virtual int lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      const common::ObNewRow& row, ObLockFlag lock_flag) = 0;

  virtual int get_concurrent_cnt(uint64_t table_id, int64_t schema_version, int64_t& concurrent_cnt) = 0;

  virtual int get_build_index_param(const uint64_t index_id, const int64_t schema_version, ObIPartitionReport* report,
      compaction::ObBuildIndexParam& param) = 0;

  virtual int get_build_index_context(
      const compaction::ObBuildIndexParam& param, compaction::ObBuildIndexContext& context) = 0;

  virtual int local_sort_index_by_range(const int64_t idx, const compaction::ObBuildIndexParam& param,
      const compaction::ObBuildIndexContext& context) = 0;

  virtual int check_index_need_build(const share::schema::ObTableSchema& index_schema, bool& need_build) = 0;

  virtual void set_merge_status(bool merge_success) = 0;
  virtual bool can_schedule_merge() = 0;

  virtual int lock(const ObStoreCtx& ctx) = 0;

  virtual int get_batch_rows(const ObTableScanParam& param, const storage::ObBatch& batch, int64_t& logical_rows,
      int64_t& physcial_rows, common::ObIArray<common::ObEstRowCountRecord>& est_records) = 0;

  virtual int get_table_stat(const uint64_t table_id, common::ObTableStat& stat) = 0;

  virtual const common::ObPartitionKey& get_partition_key() const = 0;
  virtual bool has_memstore() = 0;
  virtual const share::schema::ObMultiVersionSchemaService* get_schema_service() const = 0;

  // freeze actions
  virtual int get_partition_ss_store_info(common::ObArray<PartitionSSStoreInfo>& partition_ss_store_info_list) = 0;
  // virtual int get_sstore_min_version(common::ObVersion &min_version) = 0;
  virtual int retire_warmup_store(const bool is_disk_full) = 0;
  virtual int halt_prewarm() = 0;
  virtual int update_multi_version_start(const int64_t multi_version_start) = 0;

  virtual int get_replayed_table_version(int64_t& table_version)
  {
    UNUSED(table_version);
    return common::OB_NOT_SUPPORTED;
  }

  virtual bool is_temporary_replica() const
  {
    return false;
  }

  virtual int set_replica_status(const int64_t status, const bool write_redo_log)
  {
    UNUSED(status);
    UNUSED(write_redo_log);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int check_index_build_status(bool& available) const
  {
    available = false;
    return common::OB_NOT_SUPPORTED;
  }
  virtual int do_warm_up_request(const ObIWarmUpRequest* request) = 0;

  virtual common::ObReplicaType get_replica_type() = 0;

  /*
    SPLIT the input ranges into group based on their corresponding MACROS

    allocator         IN			allocator used to deep copy ranges
    ranges            IN   		all input ranges
    type						  IN      decide the performance of this function,
                                                        it can be OB_GET_MARCOS_COUNT_BY_QUERY_RANGE or
    OB_GET_BLOCK_RANGE when type == OB_GET_MARCOS_COUNT_BY_QUERY_RANGE, we can get macros count by var macros_count.
    type == OB_GET_BLOCK_RANGE, we can get the whole block ranges.
    macros_count		  OUT			the macros count belong to these ranges
    total_task_count  IN     	total_task_count
    splitted_ranges   OUT   	all output ranges
    split_index       OUT   	position of the last range of the group, one
                                for each group
   */
  virtual int query_range_to_macros(common::ObIAllocator& allocator,
      const common::ObIArray<common::ObStoreRange>& ranges, const int64_t type, uint64_t* macros_count,
      const int64_t* total_task_count, ObIArray<common::ObStoreRange>* splitted_ranges,
      common::ObIArray<int64_t>* split_index) = 0;
  virtual int get_multi_ranges_cost(const common::ObIArray<common::ObStoreRange>& ranges, int64_t& total_size) = 0;
  virtual int split_multi_ranges(const common::ObIArray<common::ObStoreRange>& ranges,
      const int64_t expected_task_count, common::ObIAllocator& allocator,
      common::ObArrayArray<common::ObStoreRange>& multi_range_split_array) = 0;

  virtual int append_local_sort_data(const share::ObBuildIndexAppendLocalDataParam& param,
      const common::ObPGKey& pg_key, const blocksstable::ObStorageFileHandle& file_handle,
      common::ObNewRowIterator& iter) = 0;

  virtual int get_all_tables(ObTablesHandle& tables_handle) = 0;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_I_PARTITION_STORAGE_
