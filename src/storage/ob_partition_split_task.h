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

#ifndef OB_PARTITION_SPLIT_TASK_H_
#define OB_PARTITION_SPLIT_TASK_H_

#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ob_partition_merge_task.h"
#include "storage/ob_pg_mgr.h"
#include "storage/ob_macro_block_iterator.h"
#include "storage/blocksstable/ob_macro_block.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_lob_merge_writer.h"
#include "storage/blocksstable/ob_lob_data_reader.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace storage {
class ObIPartitionReport;
class ObMultiVersionRowInfo;

struct ObSSTableScheduleSplitParam {
  ObSSTableScheduleSplitParam();
  bool is_valid() const;
  TO_STRING_KV(K_(is_major_split), K_(frozen_version), K_(merge_version), K_(src_pkey), K_(dest_pkey), K_(index_id));

  bool is_major_split_;
  common::ObVersion frozen_version_;
  common::ObVersion merge_version_;
  common::ObPartitionKey src_pkey_;
  common::ObPartitionKey dest_pkey_;
  common::ObPartitionKey dest_pg_key_;
  uint64_t index_id_;
};

struct ObSSTableSplitCtx {
  ObSSTableSplitCtx();
  virtual ~ObSSTableSplitCtx()
  {}

  virtual bool is_valid() const;

  // 1. filled in ObPartitionScheduler::schedule_split_sstable_dag
  ObSSTableScheduleSplitParam param_;
  storage::ObIPartitionReport* report_;

  // 2. filled in ObPartitionStore::get_split_tables
  storage::ObTablesHandle tables_handle_;

  // 3. filled in ObPartitionStorage::cal_split_param
  common::ObVersion base_sstable_version_;

  // 4. filled in ObPartitionStorage::get_concurrent_cnt
  int64_t concurrent_count_;
  storage::ObIPartitionGroupGuard partition_guard_;
  int64_t checksum_method_;

  // 5. filled in ObPartitionStorage::get_schemas_to_split
  int64_t schema_version_;
  int64_t split_schema_version_;
  const share::schema::ObTableSchema* table_schema_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  const share::schema::ObTableSchema* split_table_schema_;
  share::schema::ObSchemaGetterGuard split_schema_guard_;

  // 6. filled in ObPartitionStorage::split_sstable, ObSSTable::fill_split_handles
  // split sstable, and remain sstable, left 1/3 and right 2/3
  storage::ObTableHandle split_handle_;
  storage::ObTableHandle remain_handle_;

  // 7. filled in ObPartitionStorage::build_split_ctx
  int64_t split_cnt_;
  // 8. filled in ObSSTableSplitPrepareTask::process
  int64_t column_cnt_;
  memtable::ObIMemtableCtxFactory* mem_ctx_factory_;
  bool is_range_opt_;
  TO_STRING_KV(K_(param), K_(tables_handle), K_(base_sstable_version), K_(concurrent_count), K_(checksum_method),
      K_(schema_version), K_(split_schema_version), KP_(table_schema), KP_(split_table_schema), K_(split_handle),
      K_(remain_handle), K_(split_cnt), K_(column_cnt), K_(is_range_opt));
};

class ObSSTableSplitDag : public share::ObIDag {
public:
  ObSSTableSplitDag();
  virtual ~ObSSTableSplitDag();
  ObSSTableSplitCtx& get_ctx()
  {
    return ctx_;
  }
  virtual bool operator==(const share::ObIDag& other) const override;
  virtual int64_t hash() const override;
  virtual int init(const ObSSTableScheduleSplitParam& param, memtable::ObIMemtableCtxFactory* mem_ctx_factory,
      ObIPartitionReport* report);
  virtual int64_t get_tenant_id() const override
  {
    return ctx_.param_.dest_pkey_.get_tenant_id();
  }
  virtual int fill_comment(char* buf, const int64_t buf_len) const override;
  virtual int64_t get_compat_mode() const override
  {
    return static_cast<int64_t>(compat_mode_);
  }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, "param", ctx_);

private:
  bool is_inited_;
  share::ObWorker::CompatMode compat_mode_;
  ObSSTableSplitCtx ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableSplitDag);
};

class ObSSTableSplitPrepareTask : public share::ObITask {
public:
  ObSSTableSplitPrepareTask();
  virtual ~ObSSTableSplitPrepareTask()
  {}
  int init();
  virtual int process();

private:
  int generate_split_task(const bool has_lob_column);
  int build_split_ctx(ObPartitionStorage& storage, storage::ObSSTableSplitCtx& ctx);
  int get_schemas_to_split(storage::ObSSTableSplitCtx& ctx);
  int cal_split_param(storage::ObSSTableSplitCtx& ctx);
  int build_split_sstable_handle(storage::ObSSTableSplitCtx& ctx);

private:
  ObSSTableSplitDag* split_dag_;
  bool is_inited_;
};

class ObLobSplitHelper {
public:
  enum ObLobSplitHelperStatus { STATUS_INVALID = 0, STATUS_WRITE = 1, STATUS_READ = 2, STATUS_EMPTY = 3 };
  ObLobSplitHelper();
  virtual ~ObLobSplitHelper();
  int init(const blocksstable::ObDataStoreDesc& data_store_desc);
  void reset();
  int overflow_lob_objs(const ObStoreRow* row, const ObStoreRow*& result_row);
  int read_lob_columns(ObStoreRow* store_row);
  int switch_for_read();

private:
  OB_INLINE bool is_valid() const
  {
    return status_ != STATUS_INVALID;
  }
  int build_lob_block_id_map();

private:
  blocksstable::ObLobMergeWriter lob_writer_;
  blocksstable::ObLobDataReader lob_reader_;
  common::hash::ObCuckooHashMap<common::ObLogicMacroBlockId, blocksstable::MacroBlockId> lob_block_id_map_;
  common::ObArenaAllocator allocator_;
  ObLobSplitHelperStatus status_;
};

// split task for one sstable
class ObSSTableSplitTask : public share::ObITask {
public:
  ObSSTableSplitTask();
  virtual ~ObSSTableSplitTask();

  int init(const int64_t idx, const bool has_lob_column);
  int generate_next_task(ObITask*& next_task);
  virtual int process();

private:
  int build_row_iterator(ObSSTableSplitCtx& split_ctx, ObIStoreRowIterator*& row_iter);
  int split_sstable(ObSSTableSplitCtx& split_ctx, ObSSTable* sstable, storage::ObMacroBlockIterator& macro_block_iter);
  int split_sstable(ObSSTableSplitCtx& split_ctx, ObIStoreRowIterator* row_iter);
  int fill_macro_blocks(const int64_t partition_id);
  int fill_finish_task(const int64_t child_idx, blocksstable::ObMacroBlocksWriteCtx& macro_blocks,
      blocksstable::ObMacroBlocksWriteCtx& lob_macro_blocks);
  int convert_row_with_part_id(const ObStoreRow* in_row, const int64_t part_id, ObStoreRow* out_row);
  int convert_row_without_part_id(const ObStoreRow* in_row, ObStoreRow* out_row);
  int overflow_if_needed(const ObStoreRow* base_row, ObLobSplitHelper* lob_helper, const ObStoreRow*& target_row);
  int split_row_by_row(ObSSTableSplitCtx& split_ctx, ObIStoreRowIterator*& row_iter);
  int split_range_by_range(ObSSTableSplitCtx& split_ctx);
  int init_table_scan_param(ObSSTableSplitCtx& split_ctx);
  int init_table_split_param(ObSSTableSplitCtx& split_ctx);
  int terminate_split_iteration(const int64_t partition_id);

private:
  static const int64_t DEFAULT_SPLIT_SORT_MEMORY_LIMIT = 64L * 1024L * 1024L;
  ObSSTableSplitDag* split_dag_;
  ObArenaAllocator allocator_;
  bool is_major_split_;
  int64_t split_cnt_;

  ObTableAccessParam param_;
  ObTableAccessContext context_;
  storage::ObStoreCtx store_ctx_;
  common::ObExtStoreRange range_;
  blocksstable::ObBlockCacheWorkingSet block_cache_ws_;

  ObStoreRow* row_with_part_id_;
  ObStoreRow* row_without_part_id_;
  common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> partition_index_;
  blocksstable::ObMacroBlockWriter writer_;
  bool has_lob_column_;
  blocksstable::ObDataStoreDesc desc_;
  ObMultiVersionColDescGenerate desc_generator_;
  const ObMultiVersionRowInfo* multi_version_row_info_;

  int64_t idx_;
  storage::ObMacroBlockIterator macro_block_iter_;
  sql::ObExecContext exec_ctx_;
  sql::ObSQLSessionInfo session_;
  sql::ObTableLocation table_location_;
  sql::ObSqlSchemaGuard link_schema_guard_;
  bool is_inited_;
};

// split finish task for one sstable
class ObSSTableSplitFinishTask : public share::ObITask {
public:
  ObSSTableSplitFinishTask();
  virtual ~ObSSTableSplitFinishTask();
  int init(const common::ObPartitionKey& dest_pkey, const bool has_lob_column);
  int add_macro_blocks(int64_t idx, blocksstable::ObMacroBlocksWriteCtx& blocks_ctx,
      blocksstable::ObMacroBlocksWriteCtx& lob_blocks_ctx);
  virtual int process();
  int process_range_split();
  int process_row_split();

private:
  ObSSTableSplitDag* split_dag_;
  ObSSTableMergeContext split_context_;
  ObArenaAllocator allocator_;
  common::ObPartitionKey dest_pkey_;
  bool is_major_split_;
  bool is_inited_;
};

class ObSplitRowComparer {
public:
  ObSplitRowComparer(int& comp_ret) : result_code_(comp_ret), column_cnt_(0)
  {}
  virtual ~ObSplitRowComparer()
  {}
  OB_INLINE bool operator()(const ObStoreRow* left, const ObStoreRow* right);
  int& result_code_;
  int64_t column_cnt_;
};

/**
 * --------------------------------------------------------Inline
 * Function------------------------------------------------------
 */
OB_INLINE bool ObSplitRowComparer::operator()(const ObStoreRow* left, const ObStoreRow* right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(common::OB_SUCCESS != result_code_)) {
    // do nothing
  } else if (OB_UNLIKELY(NULL == left) || OB_UNLIKELY(NULL == right) || OB_UNLIKELY(column_cnt_ <= 0)) {
    result_code_ = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invaid argument, ", KP(left), KP(right), K_(column_cnt), K_(result_code));
  } else {
    for (int64_t i = 0; OB_LIKELY(common::OB_SUCCESS == result_code_) && i < column_cnt_; ++i) {
      if (i < left->row_val_.count_ && i < right->row_val_.count_) {
        const int cmp = left->row_val_.cells_[i].compare(right->row_val_.cells_[i]);
        if (cmp < 0) {
          bool_ret = true;
          break;
        } else if (cmp > 0) {
          bool_ret = false;
          break;
        }
      } else {
        result_code_ = common::OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN,
            "index is out of bound",
            K_(result_code),
            K(left->row_val_.count_),
            K(right->row_val_.count_),
            K_(column_cnt),
            K(i));
      }
    }
  }
  return bool_ret;
}

}  // namespace storage
}  // namespace oceanbase

#endif
