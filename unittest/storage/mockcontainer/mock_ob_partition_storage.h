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

#ifndef OCEANBASE_STORAGE_MOCK_OB_PARTITION_STORAGE_H_
#define OCEANBASE_STORAGE_MOCK_OB_PARTITION_STORAGE_H_
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_partition_merge_task.h"
#include "storage/ob_replay_status.h"
#include "storage/ob_saved_storage_info.h"
#include "gmock/gmock.h"

namespace oceanbase {
namespace storage {
class MockObIPartitionStorage : public ObIPartitionStorage {
public:
  virtual ~MockObIPartitionStorage()
  {}
  MOCK_METHOD5(init,
      int(const common::ObPartitionKey &pkey,
          ObIPartitionComponentFactory *cp_fty,
          share::schema::ObMultiVersionSchemaService *schema_service,
          transaction::ObTransService *txs);
  MOCK_METHOD0(destroy, void());



  MOCK_METHOD2(table_scan,
        int(ObTableScanParam &param,
            common::ObNewRowIterator *&result));

  MOCK_METHOD4(join_mv_scan,
      int(ObTableScanParam &left_param,
          ObTableScanParam &right_param,
          ObIPartitionStorage &right_storage,
          common::ObNewRowIterator *&result));

  MOCK_METHOD1(revert_scan_iter, int(common::ObNewRowIterator *iter));

  MOCK_METHOD5(delete_rows,
      int(const ObStoreCtx &ctx,
          const ObDMLBaseParam &dml_param,
          const common::ObIArray<uint64_t> &column_ids,
          common::ObNewRowIterator *row_iter,
          int64_t &affected_rows));
  MOCK_METHOD4(delete_row,
               int(const ObStoreCtx &ctx,
                   const ObDMLBaseParam &dml_param,
                   const common::ObIArray<uint64_t> &column_ids,
                   const common::ObNewRow &row));
  MOCK_METHOD5(insert_rows,
      int(const ObStoreCtx &ctx,
          const ObDMLBaseParam &dml_param,
          const common::ObIArray<uint64_t> &column_ids,
          common::ObNewRowIterator *row_iter,
          int64_t &affected_rows));
  MOCK_METHOD8(insert_row,
      int(const ObStoreCtx &ctx,
          const ObDMLBaseParam &dml_param,
          const common::ObIArray<uint64_t> &column_ids,
          const common::ObIArray<uint64_t> &duplicated_column_ids,
          const common::ObNewRow &row,
          const ObInsertFlag flag,
          int64_t &affected_rows,
          common::ObNewRowIterator *&duplicated_rows));
  MOCK_METHOD1(revert_insert_iter,
      int(common::ObNewRowIterator *iter));

  MOCK_METHOD6(update_rows,
      int(const ObStoreCtx &ctx,
          const ObDMLBaseParam &dml_param,
          const common::ObIArray<uint64_t> &column_ids,
          const common::ObIArray< uint64_t> &updated_column_ids,
          common::ObNewRowIterator *row_iter,
          int64_t &affected_rows));
  MOCK_METHOD6(update_row,
      int(const ObStoreCtx &ctx,
          const ObDMLBaseParam &dml_param,
          const common::ObIArray<uint64_t> &column_ids,
          const common::ObIArray<uint64_t> &updated_column_ids,
          const common::ObNewRow &old_row,
          const common::ObNewRow &new_row));
  MOCK_METHOD6(lock_rows,
      int(const ObStoreCtx &ctx,
          const ObDMLBaseParam &dml_param,
          const int64_t abs_lock_timeout,
          common::ObNewRowIterator *row_iter,
          ObLockFlag lock_flag,
          int64_t &affected_rows));
  MOCK_METHOD5(lock_rows,
      int(const ObStoreCtx &ctx,
          const ObDMLBaseParam &dml_param,
          const int64_t abs_lock_timeout,
          const common::ObNewRow &row,
          ObLockFlag lock_flag));
  MOCK_METHOD2(freeze_memtable, int(const FreezeType freeze_type, uint64_t frozen_version));
  MOCK_METHOD1(migrate, int(uint64_t migrate_version ));
  MOCK_METHOD3(merge, int(uint64_t frozen_version, int64_t &merge_version, ObIPartitionReport &report));
  MOCK_METHOD3(get_concurrent_cnt, int(uint64_t table_id, int64_t schema_version, int64_t &concurrent_cnt));

  MOCK_CONST_METHOD1(can_be_minor_merged, bool(const common::ObVersion &version));
  MOCK_CONST_METHOD1(get_minor_merge_version, int(common::ObVersion &merge_version));
  MOCK_METHOD2(minor_merge, int(const common::ObVersion &start_version,
                                bool &merge));
  MOCK_CONST_METHOD1(get_merged_version, int(common::ObVersion &merged_version));
  MOCK_METHOD0(dump, int());
  MOCK_METHOD4(get_build_index_param, int(
      const uint64_t index_id,
      const int64_t schema_version,
      compaction::ObBuildIndexParam &param));
  MOCK_METHOD2(get_build_index_context, int(
      const compaction::ObBuildIndexParam &param,
      compaction::ObBuildIndexContext &context));
  MOCK_METHOD3(local_sort_index_by_range, int(
      const int64_t idx,
      const compaction::ObBuildIndexParam &param,
      const compaction::ObBuildIndexContext &context));
  MOCK_METHOD4(add_build_index_sstable, int(
      const share::schema::ObTableSchema &index_schema,
      const ObVersion version,
      const int64_t schema_cnt,
      ObExternalSort<ObStoreRow, ObStoreRowComparer> &external_sort));
  MOCK_CONST_METHOD0(get_merge_schema_version, int64_t());
  MOCK_CONST_METHOD0(get_merge_data_version, int64_t());
  MOCK_METHOD2(check_unique_index_valid, int(
        const int64_t check_snapshot_version,
        const share::schema::ObTableSchema &index_schema));
  MOCK_METHOD2(normal_minor_merge, int (
        const ObVersion &start_version,
        bool &is_major_freeze_store));
  MOCK_METHOD3(build_index_minor_merge, int(
      const ObVersion &start_version,
      const ObIArray<uint64_t> &to_minor_merge_tables,
      bool &is_major_freeze_store));
  MOCK_METHOD4(merge_local_sort_index, int(
      const compaction::ObBuildIndexParam &param,
      const ObIArray<ObExternalSort<ObStoreRow, ObStoreRowComparer> *> &local_sorters,
      ObExternalSort<ObStoreRow, ObStoreRowComparer> &merge_sorter,
      compaction::ObBuildIndexContext *context));
  MOCK_METHOD2(check_index_need_build, int(const share::schema::ObTableSchema &index_schema, bool &need_build));
  MOCK_METHOD2(set_build_index_ret, int(const int64_t index_id, const int ret_code));

  MOCK_METHOD0(retire_sorted_store, int());
  MOCK_METHOD1(halt_prewarm, int(bool only_clear_memstore));
  MOCK_METHOD1(purge_retire_stores, int(uint64_t &last_replay_log_id));

  MOCK_METHOD1(set_merge_status, void(bool merge_success));
  MOCK_METHOD0(can_schedule_merge, bool());
  MOCK_METHOD2(remove_store, int(const common::ObVersion &version, const bool write_redo_log));
  MOCK_METHOD3(add_ssstores,
               int(const ObIArray<ObSSStore *> &ssstores,
                   const bool clear_all_stores,
                   const bool to_empty_stores));

  MOCK_METHOD0(adjust_freeze_status, int());
  MOCK_METHOD1(get_status, enum ObPartitionStorageStatus(const uint64_t frozen_version));
  MOCK_METHOD1(get_all_tables, int (ObTablesHandle &handle));
  MOCK_METHOD1(save_base_storage_info, int(const common::ObBaseStorageInfo &info));
  MOCK_METHOD1(get_base_storage_info, int(common::ObBaseStorageInfo &info));
  MOCK_METHOD3(get_saved_storage_info,
      int(const ObStoreType &type, ObSavedStorageInfo &info, common::ObVersion &version));
  MOCK_METHOD1(get_store_infos, int(ObIArray<ObStoreInfo> &store_infos));
  MOCK_METHOD1(save_frozen_base_storage_info, int(const ObSavedStorageInfo &info));


  // leader or follower
  MOCK_METHOD1(lock, int(const ObStoreCtx &ctx));

  MOCK_METHOD2(get_scan_cost, int(const ObTableScanParam &param,
                                        ObPartitionEst &cost_estimate));

  MOCK_METHOD5(get_batch_rows, int(const ObTableScanParam &param,
                                   const storage::ObBatch &batch,
                                   int64_t &rows,
                                   int64_t &rows_unreliable,
                                   common::ObIArray<common::ObEstRowCountRecord> &est_records));


  MOCK_METHOD2(get_table_stat, int(const uint64_t table_id, common::ObTableStat &stat));

  // write ssstore objects @version tree to data file , used by write_check_point
  MOCK_METHOD3(serialize,
      int (ObArenaAllocator &allocator, char *&new_buf, int64_t &serialize_size));

  // read ssstore objects from data file to construct partition storage's version tree.
  MOCK_METHOD3(deserialize,
      int (const char *buf, const int64_t buf_len, int64_t &pos));

  MOCK_METHOD0(get_serialize_size, int64_t());


  MOCK_METHOD0( get_storage_log_seq_num, int64_t());
  MOCK_CONST_METHOD0 (get_partition_key, const common::ObPartitionKey &());
  MOCK_CONST_METHOD0 (get_replication_group_key, const ObRGKey &());
  MOCK_METHOD0 (has_memstore, bool());
  MOCK_METHOD0 (get_memstore_allocator, common::ObIAllocator &());
  MOCK_CONST_METHOD0 (get_schema_service, const share::schema::ObMultiVersionSchemaService *());

  MOCK_METHOD5(replay_base_storage_log, int(const int64_t log_seq_num,
      const int64_t subcmd,
      const char *buf,
      const int64_t len,
      int64_t &pos));

  MOCK_METHOD1(pre_alloc_memstore, int(const bool is_minor_freeze));
  MOCK_METHOD1(new_active_memstore, int(const ObSavedStorageInfo &info));
  MOCK_METHOD1(effect_new_active_memstore, int(common::ObVersion &version));
  MOCK_METHOD0(clean_new_active_memstore, int());
  MOCK_METHOD1(get_partition_ss_store_info, int(common::ObArray<PartitionSSStoreInfo> &partition_ss_store_info_list));
  MOCK_METHOD1(get_sstore_min_version, int(common::ObVersion &min_version));
  MOCK_METHOD1(do_warm_up_request, int(const oceanbase::storage::ObIWarmUpRequest*));
  MOCK_METHOD2(check_store_can_replay,
               int (ObSSStore &store, bool &replay));
  MOCK_METHOD2(set_replica_type, int(const common::ObReplicaType &replica_type,
        const bool write_redo_log));
  MOCK_METHOD0(get_replica_type, common::ObReplicaType());
  MOCK_METHOD5(split_macros, int(common::ObIAllocator &allocator,
                                 const common::ObIArray<common::ObStoreRange> &ranges,
                                 const int64_t parallelism,
                                 ObIArray<common::ObStoreRange> &splitted_ranges,
                                 common::ObIArray<int64_t> &split_index));
  MOCK_METHOD2(append_local_sort_data,
      int(
        const share::ObBuildIndexAppendLocalDataParam &param,
        common::ObNewRowIterator &iter));
  MOCK_METHOD3(append_sstable,
      int(
        const share::ObBuildIndexAppendSSTableParam &param,
        common::ObNewRowIterator &iter,
        ObSSStore *&store));
  MOCK_METHOD7(query_range_to_macros, int(common::ObIAllocator &allocator,
  																		const common::ObIArray<common::ObStoreRange> &ranges,
																			const int64_t type,
																			uint64_t *macros_count,
																			const int64_t *parallelism,
																			ObIArray<common::ObStoreRange> *splitted_ranges,
																			common::ObIArray<int64_t> *split_index));
  MOCK_METHOD2(get_migrating_store, int(const common::ObVersion &migrate_version,
                                    ObTablesHandle &handle));
  virtual int put_rows(const ObStoreCtx &ctx,
                       const ObDMLBaseParam &dml_param,
                       const common::ObIArray<uint64_t> &column_ids,
                       common::ObNewRowIterator *row_iter,
                       int64_t &affected_rows) override
  {
    UNUSED(ctx);
    UNUSED(dml_param);
    UNUSED(column_ids);
    UNUSED(row_iter);
    UNUSED(affected_rows);
    return OB_NOT_SUPPORTED;
  }
};

}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_MOCK_OB_PARTITION_H_
