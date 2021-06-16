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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_TAG_DELETE_MAKER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_TAG_DELETE_MAKER_H_

#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_block_cache_working_set.h"
#include "storage/ob_multiple_scan_merge.h"

namespace oceanbase {
namespace blocksstable {

class ObBlockMarkDeletionMaker {
public:
  typedef common::ObSEArray<int32_t, common::OB_DEFAULT_COL_DEC_NUM> OutColsProject;
  static const int64_t MAX_REUSE_MEMORY_LIMIT = 100;

public:
  ObBlockMarkDeletionMaker();
  virtual ~ObBlockMarkDeletionMaker();
  int init(const share::schema::ObTableSchema& table_schema, const common::ObPartitionKey& pkey,
      const uint64_t index_id, const int64_t snapshot_version, const uint64_t end_log_id);
  void reset();
  int can_mark_delete(bool& can_mark_deletion, common::ObExtStoreRange& range);

private:
  int prepare_tables_(const common::ObPartitionKey& pkey, const uint64_t index_id, const int64_t snapshot_version,
      const uint64_t end_log_id);
  int init_(const share::schema::ObTableSchema& table_schema, const common::ObPartitionKey& pkey,
      const uint64_t index_id, const int64_t snapshot_version, memtable::ObIMemtableCtxFactory* ctx_factory);

private:
  bool is_inited_;
  storage::ObTableAccessParam access_param_;
  storage::ObTableAccessContext access_context_;
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator stmt_allocator_;
  storage::ObStoreCtx ctx_;
  ObBlockCacheWorkingSet block_cache_ws_;
  storage::ObMultipleScanMerge scan_merge_;
  OutColsProject out_cols_project_;
  storage::ObTablesHandle tables_handle_;
  memtable::ObIMemtableCtxFactory* ctx_factory_;
  memtable::ObIMemtableCtx* mem_ctx_;
  ObSEArray<share::schema::ObColumnParam*, OB_DEFAULT_SE_ARRAY_COUNT> col_params_;
  common::ObPartitionKey pkey_;
  DISALLOW_COPY_AND_ASSIGN(ObBlockMarkDeletionMaker);
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_TAG_DELETE_MAKER_H_ */
