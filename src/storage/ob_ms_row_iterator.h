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

#ifndef OCEANBASE_STORAGE_MS_ROW_ITERATOR_H
#define OCEANBASE_STORAGE_MS_ROW_ITERATOR_H

#include "compaction/ob_partition_merge_util.h"
#include "storage/ob_partition_merge_task.h"
#include "compaction/ob_partition_merge.h"

namespace oceanbase {
namespace storage {

class ObMSRowIterator : public ObIStoreRowIterator {
public:
  ObMSRowIterator();
  ~ObMSRowIterator();
  int init(ObTablesHandle& tables_handle, const share::schema::ObTableSchema& table_schema, ObExtStoreRange& range,
      const ObVersionRange& version_range, memtable::ObIMemtableCtxFactory* memctx_factory,
      const common::ObPartitionKey& pg_key);
  int prepare_iterators(memtable::ObIMemtableCtxFactory* memctx_factory, common::ObExtStoreRange& range,
      const common::ObPartitionKey& pg_key);
  void reuse();
  int get_next_row(const storage::ObStoreRow*& store_row);

private:
  int init_merge_param(ObTablesHandle& tables_handle, const share::schema::ObTableSchema& table_schema,
      const ObVersionRange& version_range);
  int inner_get_next();

private:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  compaction::ObIPartitionMergeFuser::MERGE_ITER_ARRAY macro_row_iters_;  // Store all macro_row_iter corresponding to
                                                                          // ssstore
  compaction::ObIPartitionMergeFuser::MERGE_ITER_ARRAY minimum_iters_;  // Multi-way merge and store the macro_row_iter
                                                                        // with the smallest rowkey
  compaction::ObSparseMinorPartitionMergeFuser sparse_partition_fuser_;
  compaction::ObFlatMinorPartitionMergeFuser flat_partition_fuser_;
  compaction::ObMinorPartitionMergeFuser* partition_fuser_;
  ObMergeParameter merge_param_;
  ObRowDml cur_first_dml_;
  DISALLOW_COPY_AND_ASSIGN(ObMSRowIterator);
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_MS_ROW_ITERATOR_H_ */
