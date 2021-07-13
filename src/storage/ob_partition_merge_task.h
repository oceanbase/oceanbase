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

#ifndef OB_PARTITION_MERGE_TASK_H_
#define OB_PARTITION_MERGE_TASK_H_

#include "storage/ob_storage_struct.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "lib/utility/ob_print_utils.h"
#include "common/rowkey/ob_rowkey.h"
#include "storage/ob_i_partition_report.h"
#include "storage/compaction/ob_partition_merge_util.h"
#include "storage/compaction/ob_column_checksum_calculator.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_partition.h"
#include "storage/ob_partition_parallel_merge_ctx.h"

namespace oceanbase {

namespace common {
class ObStoreRowkey;
}

namespace storage {
class ObSSTableMergeDag;

class ObMacroBlockMergeTask : public share::ObITask {
public:
  ObMacroBlockMergeTask();
  virtual ~ObMacroBlockMergeTask();
  int init(const int64_t idx, storage::ObSSTableMergeCtx& ctx);
  virtual int process();
  int generate_next_task(ObITask*& next_task);

private:
  int process_iter_to_complement(transaction::ObTransService* txs);

private:
  int64_t idx_;
  storage::ObSSTableMergeCtx* ctx_;
  compaction::ObMacroBlockBuilder builder_;
  compaction::ObMacroBlockEstimator estimator_;
  compaction::ObIStoreRowProcessor* processor_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockMergeTask);
};

// used for record output macro blocks
class ObSSTableMergeContext {
public:
  ObSSTableMergeContext();
  virtual ~ObSSTableMergeContext();

  int init(const int64_t array_count, const bool has_lob, common::ObIArray<common::ObColumnStat*>* column_stats,
      const bool merge_complement);
  int add_macro_blocks(const int64_t idx, blocksstable::ObMacroBlocksWriteCtx* blocks_ctx,
      blocksstable::ObMacroBlocksWriteCtx* lob_blocks_ctx, const ObSSTableMergeInfo& sstable_merge_info);
  int add_bloom_filter(blocksstable::ObMacroBlocksWriteCtx& bloom_filter_blocks_ctx);
  int add_column_stats(const common::ObIArray<common::ObColumnStat*>& column_stats);
  int create_sstable(storage::ObCreateSSTableParamWithTable& param, storage::ObIPartitionGroupGuard& pg_guard,
      ObTableHandle& table_handle);
  int create_sstables(ObIArray<storage::ObCreateSSTableParamWithTable>& params,
      storage::ObIPartitionGroupGuard& pg_guard, ObTablesHandle& tables_handle);
  int64_t get_finish_count() const
  {
    return finish_count_;
  }
  ObSSTableMergeInfo& get_sstable_merge_info()
  {
    return sstable_merge_info_;
  }
  int64_t get_file_id() const
  {
    return file_id_;
  }
  void destroy();
  int get_data_macro_block_count(int64_t& macro_block_count);
  TO_STRING_KV(K_(is_inited), K_(concurrent_cnt), K_(finish_count), K_(sstable_merge_info));

private:
  int add_lob_macro_blocks(const int64_t idx, blocksstable::ObMacroBlocksWriteCtx* blocks_ctx);
  int new_block_write_ctx(blocksstable::ObMacroBlocksWriteCtx*& ctx);
  OB_INLINE bool need_lob_macro_blocks()
  {
    return !lob_block_ctxs_.empty();
  }

private:
  bool is_inited_;
  common::ObSpinLock lock_;
  ObArray<blocksstable::ObMacroBlocksWriteCtx*> block_ctxs_;
  // index sstable should not have lob blocks
  ObArray<blocksstable::ObMacroBlocksWriteCtx*> lob_block_ctxs_;
  blocksstable::ObMacroBlocksWriteCtx* bloom_filter_block_ctx_;
  ObSSTableMergeInfo sstable_merge_info_;
  common::ObIArray<common::ObColumnStat*>* column_stats_;
  common::ObArenaAllocator allocator_;
  int64_t finish_count_;
  int64_t concurrent_cnt_;
  bool merge_complement_;
  int64_t file_id_;
};

struct ObSSTableScheduleMergeParam {
  ObSSTableScheduleMergeParam();
  bool is_valid() const;

  OB_INLINE bool is_major_merge() const
  {
    return storage::is_major_merge(merge_type_);
  }
  OB_INLINE bool is_history_mini_minor_merge() const
  {
    return storage::is_history_mini_minor_merge(merge_type_);
  }
  OB_INLINE bool is_mini_merge() const
  {
    return storage::is_mini_merge(merge_type_);
  }
  OB_INLINE bool is_multi_version_minor_merge() const
  {
    return storage::is_multi_version_minor_merge(merge_type_);
  }
  OB_INLINE bool is_memtable_merge() const
  {
    return MINI_MERGE == schedule_merge_type_;
  }
  OB_INLINE bool is_mini_minor_merge() const
  {
    return storage::is_mini_minor_merge(merge_type_);
  }
  OB_INLINE bool is_minor_merge() const
  {
    return MINI_MINOR_MERGE == merge_type_ || MINOR_MERGE == merge_type_;
  }
  TO_STRING_KV(K_(merge_type), K_(merge_version), K_(pkey), K_(index_id), K_(schedule_merge_type), K_(pg_key));

  ObMergeType merge_type_;
  common::ObVersion merge_version_;
  common::ObPartitionKey pkey_;
  uint64_t index_id_;
  ObMergeType schedule_merge_type_;
  ObPartitionKey pg_key_;
};

class ObMergeParameter;
struct ObSSTableMergeCtx {
  ObSSTableMergeCtx();
  virtual ~ObSSTableMergeCtx();
  virtual bool is_valid() const;
  bool need_incremental_checksum() const
  {
    return checksum_method_ == blocksstable::CCM_TYPE_AND_VALUE;
  }
  bool need_full_checksum() const
  {
    return is_in_progressive_new_checksum_ || checksum_method_ == blocksstable::CCM_VALUE_ONLY || is_full_merge_;
  }
  bool need_rewrite_macro_block(const ObMacroBlockDesc& block_desc) const;
  int get_storage_format_version(int64_t& storage_format_version) const;
  int get_storage_format_work_version(int64_t& storage_format_work_version) const;
  int init_parallel_merge();
  int get_merge_range(int64_t parallel_idx, common::ObExtStoreRange& merge_range, common::ObIAllocator& allocator);
  OB_INLINE int64_t get_concurrent_cnt() const
  {
    return parallel_merge_ctx_.get_concurrent_cnt();
  }
  ObITable::TableType get_merged_table_type() const;
  storage::ObSSTableMergeContext& get_merge_context(const bool iter_complement)
  {
    return iter_complement ? merge_context_for_complement_minor_sstable_ : merge_context_;
  }

  // 1. init in dag
  ObSSTableScheduleMergeParam param_;
  storage::ObIPartitionReport* report_;

  // 2. filled in ObPartitionStore::get_merge_tables
  ObVersionRange sstable_version_range_;  // version range for new sstable
  int64_t create_snapshot_version_;
  int64_t dump_memtable_timestamp_;
  storage::ObTablesHandle tables_handle_;
  storage::ObTableHandle base_table_handle_;

  // 3. filled in ObPartitionStorage::get_schemas_to_merge
  int64_t base_schema_version_;
  const share::schema::ObTableSchema* mv_dep_table_schema_;  // not null means materialized_view
  const share::schema::ObTableSchema* data_table_schema_;
  common::ObSEArray<share::schema::ObIndexTableStat, OB_MAX_INDEX_PER_TABLE> index_stats_;
  int64_t schema_version_;
  const share::schema::ObTableSchema* table_schema_;  // table's schema need merge or split
  share::schema::ObSchemaGetterGuard schema_guard_;
  int64_t bf_rowkey_prefix_;

  // 4. filled in ObPartitionStorage::cal_merge_param
  bool is_full_merge_;  // full merge or increment merge
  int64_t stat_sampling_ratio_;
  storage::ObMergeLevel merge_level_;
  storage::ObMultiVersionRowInfo multi_version_row_info_;
  storage::ObSSTableMergeContext merge_context_;
  storage::ObSSTableMergeContext merge_context_for_complement_minor_sstable_;
  int64_t progressive_merge_num_;
  int64_t progressive_merge_start_version_;
  // progressive_merge_end_version_ = progressive_merge_start_version_ + progressive_merge_num_
  // current_round = merge_version - progressive_merge_start_version_
  // int64_t concurrent_count_;
  ObParallelMergeCtx parallel_merge_ctx_;

  storage::ObIPartitionGroupGuard pg_guard_;
  storage::ObPGPartitionGuard partition_guard_;

  int64_t checksum_method_;

  // 5. filled in ObPartitionStorage::prepare_merge_mv_depend_sstable
  storage::ObTablesHandle mv_dep_tables_handle_;  // obsoleted

  // 6. inited in ObSSTableMergePrepareTask::init_estimate
  common::ObArray<common::ObColumnStat*, ObIAllocator&> column_stats_;

  // 7. filled in ObSSTableMergeFinishTask::update_partition_store
  storage::ObTableHandle merged_table_handle_;
  storage::ObTableHandle merged_complement_minor_table_handle_;

  // no need init
  common::ObArenaAllocator allocator_;
  int32_t result_code_;
  compaction::ObSSTableColumnChecksum column_checksum_;
  bool is_in_progressive_new_checksum_;
  int64_t backup_progressive_merge_num_;
  int64_t backup_progressive_merge_start_version_;
  bool is_created_index_first_merge_;
  bool store_column_checksum_in_micro_;
  int64_t progressive_merge_round_;
  int64_t progressive_merge_step_;
  bool use_new_progressive_;
  bool create_sstable_for_large_snapshot_;
  int64_t logical_data_version_;
  common::ObLogTsRange log_ts_range_;
  int64_t merge_log_ts_;
  int64_t trans_table_end_log_ts_;
  int64_t trans_table_timestamp_;
  int64_t read_base_version_;  // use for major merge

  TO_STRING_KV(K_(param), K_(sstable_version_range), K_(create_snapshot_version), K_(base_schema_version),
      K_(schema_version), K_(dump_memtable_timestamp), KP_(table_schema), K_(is_full_merge), K_(stat_sampling_ratio),
      K_(merge_level), K_(progressive_merge_num), K_(progressive_merge_start_version), K_(parallel_merge_ctx),
      K_(checksum_method), K_(result_code), KP_(data_table_schema), KP_(mv_dep_table_schema), K_(index_stats),
      "tables_handle count", tables_handle_.get_count(), K_(index_stats), K_(is_in_progressive_new_checksum),
      K_(store_column_checksum_in_micro), K_(progressive_merge_round), K_(progressive_merge_step),
      K_(use_new_progressive), K_(tables_handle), K_(base_table_handle), K_(create_sstable_for_large_snapshot),
      K_(logical_data_version), K_(log_ts_range), K_(merge_log_ts), K_(trans_table_end_log_ts),
      K_(trans_table_timestamp), K_(read_base_version));

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMergeCtx);
};

// ObMergeParamater is a sub part of ObSSTableMergeCtx
struct ObMergeParameter {
  ObMergeParameter();
  ~ObMergeParameter()
  {
    reset();
  }
  bool is_valid() const;
  void reset();
  int init(ObSSTableMergeCtx& merge_ctx, const int64_t idx, const bool iter_complement);
  storage::ObTablesHandle* tables_handle_;
  bool is_full_merge_;  // full merge or increment merge
  ObMergeType merge_type_;
  int64_t checksum_method_;
  ObMergeLevel merge_level_;
  const share::schema::ObTableSchema* table_schema_;         // table's schema need merge
  const share::schema::ObTableSchema* mv_dep_table_schema_;  // not null means materialized_view
  ObVersionRange version_range_;
  compaction::ObColumnChecksumCalculator* checksum_calculator_;
  bool is_iter_complement_;

  OB_INLINE bool is_major_merge() const
  {
    return storage::is_major_merge(merge_type_);
  }
  OB_INLINE bool is_multi_version_minor_merge() const
  {
    return storage::is_multi_version_minor_merge(merge_type_);
  }
  OB_INLINE bool need_checksum() const
  {
    return storage::is_major_merge(merge_type_);
  }
  TO_STRING_KV(K_(is_full_merge), K_(merge_type), K_(checksum_method), K_(merge_level), KP_(table_schema),
      KP_(mv_dep_table_schema), K_(version_range), KP_(checksum_calculator), KP_(tables_handle),
      K_(is_iter_complement));

private:
  DISALLOW_COPY_AND_ASSIGN(ObMergeParameter);
};

class ObSSTableMergePrepareTask : public share::ObITask {
public:
  ObSSTableMergePrepareTask();
  virtual ~ObSSTableMergePrepareTask();
  int init();
  virtual int process() override;

private:
  int generate_merge_sstable_task();
  int init_estimate(storage::ObSSTableMergeCtx& ctx);
  int create_sstable_for_large_snapshot(storage::ObSSTableMergeCtx& ctx);

private:
  bool is_inited_;
  ObSSTableMergeDag* merge_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMergePrepareTask);
};

class ObSSTableMergeFinishTask : public share::ObITask {
public:
  ObSSTableMergeFinishTask();
  virtual ~ObSSTableMergeFinishTask();
  int init();
  virtual int process() override;

private:
  int get_merged_sstable_(const ObITable::TableType& type, const uint64_t table_id,
      storage::ObSSTableMergeContext& merge_context, storage::ObTableHandle& table_handle, ObSSTable*& sstable);
  int check_data_checksum();
  int check_empty_merge_valid(storage::ObSSTableMergeCtx& ctx);
  int check_macro_cnt_of_merge_table(const ObITable::TableType& type, storage::ObSSTableMergeContext& merge_context);

private:
  bool is_inited_;
  ObSSTableMergeDag* merge_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMergeFinishTask);
};

class ObWriteCheckpointTask : public share::ObITask {
public:
  ObWriteCheckpointTask();
  virtual ~ObWriteCheckpointTask();
  int init(int64_t frozen_version);
  virtual int process() override;

private:
  static const int64_t FAIL_WRITE_CHECKPOINT_ALERT_INTERVAL = 1000L * 1000L * 3600LL;  // 6h
  static const int64_t RETRY_WRITE_CHECKPOINT_MIN_INTERVAL = 1000L * 1000L * 300L;     // 5 minutes
  // Average replay time of 1 slog is 100us. Total replay time should less than 1 minute.
  // So once log count exceed 50% * (60000000 / 100) = 300000, try to write a checkpoint.
  static const int64_t MIN_WRITE_CHECKPOINT_LOG_CNT = 300000;

  bool is_inited_;
  int64_t frozen_version_;
  DISALLOW_COPY_AND_ASSIGN(ObWriteCheckpointTask);
};

class ObWriteCheckpointDag : public share::ObIDag {
public:
  ObWriteCheckpointDag();
  virtual ~ObWriteCheckpointDag();
  virtual bool operator==(const ObIDag& other) const override;
  virtual int64_t hash() const override;
  virtual int init(int64_t frozen_version);
  virtual int64_t get_tenant_id() const override;
  virtual int fill_comment(char* buf, const int64_t buf_len) const override;
  virtual int64_t get_compat_mode() const override
  {
    return static_cast<int64_t>(share::ObWorker::CompatMode::MYSQL);
  }

protected:
  bool is_inited_;
  int64_t frozen_version_;
  DISALLOW_COPY_AND_ASSIGN(ObWriteCheckpointDag);
};

class ObSSTableMergeDag : public share::ObIDag {
public:
  ObSSTableMergeDag(const ObIDagType type, const ObIDagPriority priority);
  virtual ~ObSSTableMergeDag();
  storage::ObSSTableMergeCtx& get_ctx()
  {
    return ctx_;
  }
  virtual bool operator==(const ObIDag& other) const override;
  virtual int64_t hash() const override;
  virtual int init(const ObSSTableScheduleMergeParam& param, ObIPartitionReport* report) = 0;
  virtual int64_t get_tenant_id() const override
  {
    return ctx_.param_.pkey_.get_tenant_id();
  }
  virtual int fill_comment(char* buf, const int64_t buf_len) const override;
  virtual int64_t get_compat_mode() const override
  {
    return static_cast<int64_t>(compat_mode_);
  }
  bool ignore_warning() override
  {
    return OB_NO_NEED_MERGE == dag_ret_ || OB_TABLE_IS_DELETED == dag_ret_ || OB_TENANT_HAS_BEEN_DROPPED == dag_ret_ ||
           OB_PARTITION_NOT_EXIST == dag_ret_;
  }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, "param", ctx_.param_, "sstable_version_range", ctx_.sstable_version_range_,
      "log_ts_range", ctx_.log_ts_range_);

protected:
  int inner_init(const ObSSTableScheduleMergeParam& param, ObIPartitionReport* report);

  bool is_inited_;
  share::ObWorker::CompatMode compat_mode_;
  storage::ObSSTableMergeCtx ctx_;
  // merge_type\pkey\index_id is used to check if dag is equal
  ObMergeType merge_type_;
  common::ObPartitionKey pkey_;
  uint64_t index_id_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMergeDag);
};

class ObSSTableMajorMergeDag : public ObSSTableMergeDag {
public:
  ObSSTableMajorMergeDag();
  virtual ~ObSSTableMajorMergeDag();
  int init(const ObSSTableScheduleMergeParam& param, ObIPartitionReport* report) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMajorMergeDag);
};

class ObSSTableMiniMergeDag : public ObSSTableMergeDag {
public:
  ObSSTableMiniMergeDag();
  virtual ~ObSSTableMiniMergeDag();
  int init(const ObSSTableScheduleMergeParam& param, ObIPartitionReport* report) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMiniMergeDag);
};

class ObSSTableMinorMergeDag : public ObSSTableMergeDag {
public:
  ObSSTableMinorMergeDag();
  virtual ~ObSSTableMinorMergeDag();
  int init(const ObSSTableScheduleMergeParam& param, ObIPartitionReport* report) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMinorMergeDag);
};

class ObTransTableMergeDag : public share::ObIDag {
public:
  ObTransTableMergeDag() : ObIDag(ObIDagType::DAG_TYPE_TRANS_TABLE_MERGE, ObIDagPriority::DAG_PRIO_TRANS_TABLE_MERGE){};
  ~ObTransTableMergeDag(){};
  int init(const ObPartitionKey& pg_key)
  {
    this->pg_key_ = pg_key;
    return OB_SUCCESS;
  }
  virtual bool operator==(const ObIDag& other) const override
  {
    bool is_same = true;
    if (this == &other) {
    } else if (get_type() != other.get_type()) {
      is_same = false;
    } else {
      const ObTransTableMergeDag& other_dag = static_cast<const ObTransTableMergeDag&>(other);
      is_same = pg_key_ == other_dag.pg_key_;
    }
    return is_same;
  }
  virtual int64_t hash() const override
  {
    return pg_key_.hash();
  }
  virtual int64_t get_tenant_id() const override
  {
    return pg_key_.get_tenant_id();
  }
  virtual int fill_comment(char* buf, const int64_t buf_len) const override;
  virtual int64_t get_compat_mode() const override
  {
    return static_cast<int64_t>(share::ObWorker::CompatMode::MYSQL);
  }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, "pg_key", pg_key_);

private:
  ObPartitionKey pg_key_;
  DISALLOW_COPY_AND_ASSIGN(ObTransTableMergeDag);
};

class ObTransTableMergeTask : public share::ObITask {
private:
  enum class SOURCE { NONE = 0, FROM_LOCAL, FROM_REMOTE, FROM_BOTH };

public:
  ObTransTableMergeTask();
  ~ObTransTableMergeTask(){};
  int init(const ObPartitionKey& pg_key, const int64_t end_log_ts, const int64_t trans_table_seq);
  int init(const ObPartitionKey& pg_key, ObSSTable* sstable);
  virtual int process();
  int init_schema_and_writer(blocksstable::ObMacroBlockWriter& writer);
  int merge_trans_table(blocksstable::ObMacroBlockWriter& writer);
  int get_merged_trans_sstable(ObTableHandle& table_handle, blocksstable::ObMacroBlockWriter& writer);

private:
  const int64_t SCHEMA_VERSION = 1;

private:
  int update_partition_store(ObTableHandle& table_handle);
  int merge_remote_with_local(blocksstable::ObMacroBlockWriter& writer);

private:
  ObPartitionKey pg_key_;
  common::ObArenaAllocator allocator_;
  ObPartitionKey trans_table_pkey_;
  blocksstable::ObDataStoreDesc desc_;
  int64_t end_log_ts_;
  ObTablesHandle tables_handle_;
  share::schema::ObTableSchema table_schema_;
  ObIPartitionGroupGuard pg_guard_;
  int64_t trans_table_seq_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTransTableMergeTask);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OB_PARTITION_MERGE_TASK_H_ */
