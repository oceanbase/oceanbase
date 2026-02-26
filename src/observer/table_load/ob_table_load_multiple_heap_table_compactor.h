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

#pragma once

#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadMultipleHeapTableSorter;
} // namespace storage
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadStoreTableCtx;
class ObTableLoadMergeMemSortOp;

class ObTableLoadMultipleHeapTableCompactCompare
{
public:
  ObTableLoadMultipleHeapTableCompactCompare();
  ~ObTableLoadMultipleHeapTableCompactCompare();
  bool operator()(const storage::ObDirectLoadTableHandle lhs,
                  const storage::ObDirectLoadTableHandle rhs);
  int get_error_code() const { return result_code_; }
  int result_code_;
};

class ObTableLoadMultipleHeapTableCompactor
{
  class CompactTaskProcessor;
  class CompactTaskCallback;

public:
  ObTableLoadMultipleHeapTableCompactor();
  virtual ~ObTableLoadMultipleHeapTableCompactor();
  void reset();
  int init(ObTableLoadMergeMemSortOp *op);
  int start();
  void stop();

  void set_has_error() { mem_ctx_.has_error_ = true; }

private:
  int construct_compactors();
  int start_sort();
  int finish();
  int handle_compact_task_finish(int ret_code);
  int build_result_for_heap_table();

private:
  int add_tablet_table(const storage::ObDirectLoadTableHandle &table_handle);

private:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadStoreTableCtx *store_table_ctx_;
  ObTableLoadMergeMemSortOp *op_;
  common::ObArenaAllocator allocator_; //需要最后析构
  storage::ObDirectLoadMemContext mem_ctx_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
