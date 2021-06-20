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

#ifndef OCEANBASE_STORAGE_OB_BUILD_INDEX_TASK_H_
#define OCEANBASE_STORAGE_OB_BUILD_INDEX_TASK_H_

#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_i_partition_report.h"
#include "storage/ob_store_row_comparer.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/compaction/ob_partition_merge_util.h"
#include "storage/ob_pg_partition.h"

namespace oceanbase {
namespace storage {
class ObIndexLocalSortTask;
class ObIndexMergeTask;
class ObCompactToLatestTask;
class ObUniqueCheckingTask;
class ObReportIndexStatusTask;

class ObBuildIndexDag : public share::ObIDag {
public:
  ObBuildIndexDag();
  virtual ~ObBuildIndexDag();
  int init(const ObPartitionKey& pkey, storage::ObPartitionService* partition_service);
  virtual int64_t hash() const;
  virtual bool operator==(const share::ObIDag& other) const;
  virtual int prepare();
  virtual int clean_up();
  int get_partition_storage(ObPartitionStorage*& storage);
  int get_partition_service(ObPartitionService*& part_service);
  int get_partition(ObPGPartition*& partition);
  int get_partition_group(ObIPartitionGroup*& pg);
  int get_pg(ObIPartitionGroup*& pg);
  bool is_inited() const
  {
    return is_inited_;
  }
  const ObPartitionKey get_partition_key() const
  {
    return pkey_;
  }
  compaction::ObBuildIndexParam& get_param()
  {
    return param_;
  }
  compaction::ObBuildIndexContext& get_context()
  {
    return context_;
  }
  int check_index_need_build(bool& need_build);

  virtual int64_t get_tenant_id() const override
  {
    return pkey_.get_tenant_id();
  }
  virtual int fill_comment(char* buf, const int64_t buf_len) const;
  virtual int64_t get_compat_mode() const override
  {
    return static_cast<int64_t>(compat_mode_);
  }

private:
  bool is_inited_;
  ObPartitionKey pkey_;
  compaction::ObBuildIndexParam param_;
  compaction::ObBuildIndexContext context_;
  storage::ObPartitionService* partition_service_;
  ObIPartitionGroupGuard pg_guard_;
  ObPGPartitionGuard guard_;
  storage::ObPartitionStorage* partition_storage_;
  share::ObWorker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObBuildIndexDag);
};

class ObIndexPrepareTask : public share::ObITask {
public:
  ObIndexPrepareTask();
  virtual ~ObIndexPrepareTask();
  int init(compaction::ObBuildIndexParam& param, compaction::ObBuildIndexContext* context);
  virtual int process();

private:
  int generate_report_index_status_task(ObBuildIndexDag* dag, ObUniqueCheckingTask* checking_task);
  int generate_compact_task(ObBuildIndexDag* dag, ObIndexMergeTask* merge_task, ObCompactToLatestTask*& compact_task);
  int generate_unique_checking_task(
      ObBuildIndexDag* dag, ObCompactToLatestTask* merge_task, ObUniqueCheckingTask*& checking_task);
  int generate_index_merge_task(
      ObBuildIndexDag* dag, ObIndexLocalSortTask* local_sort_task, ObIndexMergeTask*& merge_task);
  int generate_local_sort_tasks(ObBuildIndexDag* dag, ObIndexLocalSortTask*& local_sort_task);
  int add_monitor_info(ObBuildIndexDag* dag);

private:
  bool is_inited_;
  compaction::ObBuildIndexParam* param_;
  compaction::ObBuildIndexContext* context_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexPrepareTask);
};

class ObIndexLocalSortTask : public share::ObITask {
public:
  ObIndexLocalSortTask();
  virtual ~ObIndexLocalSortTask();
  int init(const int64_t task_id, compaction::ObBuildIndexParam& param, compaction::ObBuildIndexContext* context);
  virtual int process();

private:
  virtual int generate_next_task(share::ObITask*& next_task);

private:
  static const int64_t RETRY_INTERVAL = 100 * 1000;  // 100ms
  bool is_inited_;
  int64_t task_id_;
  compaction::ObBuildIndexParam* param_;
  compaction::ObBuildIndexContext* context_;
  ObExternalSort<ObStoreRow, ObStoreRowComparer>* local_sorter_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexLocalSortTask);
};

class ObIndexMergeTask : public share::ObITask {
public:
  ObIndexMergeTask();
  virtual ~ObIndexMergeTask();
  int init(compaction::ObBuildIndexParam& param, compaction::ObBuildIndexContext* context);
  virtual int process();

private:
  int merge_local_sort_index(const compaction::ObBuildIndexParam& param,
      const ObIArray<ObExternalSort<ObStoreRow, ObStoreRowComparer>*>& local_sorters,
      ObExternalSort<ObStoreRow, ObStoreRowComparer>& merge_sorter, compaction::ObBuildIndexContext* context,
      ObTableHandle& new_sstable);
  int add_build_index_sstable(const compaction::ObBuildIndexParam& param,
      ObExternalSort<ObStoreRow, ObStoreRowComparer>& external_sort, compaction::ObBuildIndexContext* context,
      ObTableHandle& new_sstable);
  int add_new_index_sstable(const compaction::ObBuildIndexParam& param, blocksstable::ObMacroBlockWriter* writer,
      const int64_t* column_checksum, ObTableHandle& new_sstable);

private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexMergeTask);
  bool is_inited_;
  compaction::ObBuildIndexParam* param_;
  compaction::ObBuildIndexContext* context_;
  const ObIArray<ObExternalSort<ObStoreRow, ObStoreRowComparer>*>* sorters_;
  ObExternalSort<ObStoreRow, ObStoreRowComparer> merge_sorter_;
  blocksstable::ObDataStoreDesc data_desc_;
  blocksstable::ObMacroBlockWriter writer_;
};

class ObCompactToLatestTask : public share::ObITask {
public:
  ObCompactToLatestTask();
  virtual ~ObCompactToLatestTask();
  int init(const common::ObPartitionKey& pkey, const compaction::ObBuildIndexParam& param,
      compaction::ObBuildIndexContext* context);
  virtual int process();
  static int wait_compact_to_latest(const ObPartitionKey& pkey, const uint64_t index_id);

private:
  static const int64_t RETRY_INTERVAL = 100 * 1000;  // 100ms
  DISALLOW_COPY_AND_ASSIGN(ObCompactToLatestTask);
  bool is_inited_;
  common::ObPartitionKey pkey_;
  const compaction::ObBuildIndexParam* param_;
  compaction::ObBuildIndexContext* context_;
};

class ObUniqueIndexChecker {
public:
  ObUniqueIndexChecker();
  virtual ~ObUniqueIndexChecker() = default;
  int init(ObPartitionService* part_service, const common::ObPartitionKey& pkey,
      const share::schema::ObTableSchema* data_table_schema, const share::schema::ObTableSchema* index_schema,
      const uint64_t execution_id = OB_INVALID_ID, const int64_t snapshot_version = OB_INVALID_VERSION);
  int check_unique_index(share::ObIDag* dag);
  int check_local_index(share::ObIDag* dag, ObPartitionStorage* storage, const int64_t snapshot_version);
  int check_global_index(share::ObIDag* dag, ObPartitionStorage* storage);

private:
  struct ObScanTableParam {
  public:
    ObScanTableParam()
        : data_table_schema_(NULL),
          index_schema_(NULL),
          snapshot_version_(0),
          col_ids_(NULL),
          output_projector_(NULL),
          is_scan_index_(false),
          tables_handle_(NULL)
    {}
    bool is_valid() const
    {
      return NULL != data_table_schema_ && NULL != index_schema_ && snapshot_version_ > 0 && NULL != col_ids_ &&
             NULL != output_projector_ && NULL != tables_handle_;
    }
    TO_STRING_KV(KP_(data_table_schema), KP_(index_schema), K_(snapshot_version), KP_(col_ids), KP_(output_projector),
        KP_(tables_handle));
    const share::schema::ObTableSchema* data_table_schema_;
    const share::schema::ObTableSchema* index_schema_;
    int64_t snapshot_version_;
    common::ObIArray<share::schema::ObColDesc>* col_ids_;
    common::ObIArray<int32_t>* output_projector_;
    bool is_scan_index_;
    ObTablesHandle* tables_handle_;
  };
  int wait_trans_end(share::ObIDag* dag, int64_t& snapshot_version);
  int scan_main_table_with_column_checksum(const share::schema::ObTableSchema& data_table_schema,
      const share::schema::ObTableSchema& index_schema, const int64_t snapshot_version, ObTablesHandle& tables_handle,
      common::ObIArray<int64_t>& column_checksum, int64_t& row_count);
  int scan_index_table_with_column_checksum(const share::schema::ObTableSchema& data_table_schema,
      const share::schema::ObTableSchema& index_schema, const int64_t snapshot_version, ObTablesHandle& tables_handle,
      common::ObIArray<int64_t>& column_checksum, int64_t& row_count);
  int scan_table_with_column_checksum(
      const ObScanTableParam& param, common::ObIArray<int64_t>& column_checksum, int64_t& row_count);
  int calc_column_checksum(const int64_t column_cnt, ObIStoreRowIterator& iterator,
      common::ObIArray<int64_t>& column_checksum, int64_t& row_count);
  int try_get_read_tables(
      const uint64_t table_id, const int64_t snapshot_version, share::ObIDag* dag, ObTablesHandle& tables_handle);

private:
  static const int64_t RETRY_INTERVAL = 10 * 1000L;
  bool is_inited_;
  ObPartitionService* part_service_;
  common::ObPartitionKey pkey_;
  const share::schema::ObTableSchema* index_schema_;
  const share::schema::ObTableSchema* data_table_schema_;
  uint64_t execution_id_;
  int64_t snapshot_version_;
  ObPGPartitionGuard part_guard_;
};

class ObUniqueCheckingTask : public share::ObITask {
public:
  ObUniqueCheckingTask();
  virtual ~ObUniqueCheckingTask();
  int init(const compaction::ObBuildIndexParam& param, compaction::ObBuildIndexContext* context);
  virtual int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObUniqueCheckingTask);
  bool is_inited_;
  const compaction::ObBuildIndexParam* param_;
  compaction::ObBuildIndexContext* context_;
  ObUniqueIndexChecker unique_checker_;
};

class ObReportIndexStatusTask : public share::ObITask {
public:
  ObReportIndexStatusTask();
  virtual ~ObReportIndexStatusTask();
  int init(
      const ObPartitionKey& pkey, const compaction::ObBuildIndexParam& param, compaction::ObBuildIndexContext* context);
  virtual int process();
  static int report_index_status(const common::ObPartitionKey& pkey, const compaction::ObBuildIndexParam& param,
      compaction::ObBuildIndexContext* context, bool& need_retry);

private:
  bool is_inited_;
  ObPartitionKey pkey_;
  const compaction::ObBuildIndexParam* param_;
  compaction::ObBuildIndexContext* context_;
  DISALLOW_COPY_AND_ASSIGN(ObReportIndexStatusTask);
};

class ObIUniqueCheckingCompleteCallback {
public:
  virtual int operator()(const int ret_code) = 0;
};

class ObGlobalUniqueIndexCallback : public ObIUniqueCheckingCompleteCallback {
public:
  ObGlobalUniqueIndexCallback(const common::ObPartitionKey& pkey, const uint64_t index_id);
  int operator()(const int ret_code) override;

private:
  common::ObPartitionKey pkey_;
  uint64_t index_id_;
};

class ObLocalUniqueIndexCallback : public ObIUniqueCheckingCompleteCallback {
public:
  ObLocalUniqueIndexCallback();
  int operator()(const int ret_code) override;
};

/* Unique key checking is a time-consuming operation, so the operation is executed
 * int the dag thread pool. When it finishes unique key checking, it will call a rpc to inform
 * ROOTSERVICE the result of the unique key checking result
 */
class ObUniqueCheckingDag : public share::ObIDag {
public:
  ObUniqueCheckingDag();
  virtual ~ObUniqueCheckingDag();
  int init(const common::ObPartitionKey& pkey, ObPartitionService* part_service,
      share::schema::ObMultiVersionSchemaService* schema_service, const uint64_t index_id, const int64_t schema_version,
      const uint64_t execution_id = OB_INVALID_ID, const int64_t snapshot_version = OB_INVALID_VERSION);
  ObPartitionService* get_partition_service()
  {
    return part_service_;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const share::schema::ObTableSchema* get_index_schema() const
  {
    return index_schema_;
  }
  const share::schema::ObTableSchema* get_data_table_schema() const
  {
    return data_table_schema_;
  }
  uint64_t get_execution_id() const
  {
    return execution_id_;
  }
  int64_t get_snapshot_version() const
  {
    return snapshot_version_;
  }
  int alloc_unique_checking_prepare_task(ObIUniqueCheckingCompleteCallback* callback);
  int alloc_local_index_task_callback(ObLocalUniqueIndexCallback*& callback);
  int alloc_global_index_task_callback(
      const ObPartitionKey& pkey, const uint64_t index_id, ObGlobalUniqueIndexCallback*& callback);
  virtual int64_t hash() const;
  virtual bool operator==(const share::ObIDag& other) const;
  virtual int64_t get_tenant_id() const override
  {
    return pkey_.get_tenant_id();
  }
  virtual int fill_comment(char* buf, const int64_t buf_len) const override;
  virtual int64_t get_compat_mode() const override
  {
    return static_cast<int64_t>(compat_mode_);
  }

private:
  bool is_inited_;
  ObPartitionService* part_service_;
  common::ObPartitionKey pkey_;
  ObPGPartitionGuard part_guard_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  const share::schema::ObTableSchema* index_schema_;
  const share::schema::ObTableSchema* data_table_schema_;
  ObIUniqueCheckingCompleteCallback* callback_;
  uint64_t execution_id_;
  int64_t snapshot_version_;
  share::ObWorker::CompatMode compat_mode_;
};

class ObUniqueCheckingPrepareTask : public share::ObITask {
public:
  ObUniqueCheckingPrepareTask();
  virtual ~ObUniqueCheckingPrepareTask() = default;
  int init(ObIUniqueCheckingCompleteCallback* callback);
  virtual int process() override;

private:
  int generate_unique_checking_task(ObUniqueCheckingDag* dag);

private:
  bool is_inited_;
  const share::schema::ObTableSchema* index_schema_;
  const share::schema::ObTableSchema* data_table_schema_;
  ObIUniqueCheckingCompleteCallback* callback_;
};

class ObSimpleUniqueCheckingTask : public share::ObITask {
public:
  ObSimpleUniqueCheckingTask();
  virtual ~ObSimpleUniqueCheckingTask() = default;
  int init(const share::schema::ObTableSchema* data_table_schema, const share::schema::ObTableSchema* index_schema,
      ObIUniqueCheckingCompleteCallback* callback);
  virtual int process() override;

private:
  bool is_inited_;
  ObUniqueIndexChecker unique_checker_;
  const share::schema::ObTableSchema* index_schema_;
  const share::schema::ObTableSchema* data_table_schema_;
  ObPartitionKey pkey_;
  ObIUniqueCheckingCompleteCallback* callback_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_BUILD_INDEX_TASK_H_
