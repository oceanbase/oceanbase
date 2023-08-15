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

#include "observer/table_load/ob_table_load_table_compactor.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_mem_define.h"
#include "storage/direct_load/ob_direct_load_multi_map.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table_sorter.h"

namespace oceanbase
{
namespace storage
{
  class ObDirectLoadMemDump;
}

namespace observer
{
class ObTableLoadParam;
class ObITableLoadTaskScheduler;

class ObTableLoadMultipleHeapTableCompactCompare
{
public:
  ObTableLoadMultipleHeapTableCompactCompare();
  ~ObTableLoadMultipleHeapTableCompactCompare();
  bool operator()(const storage::ObDirectLoadMultipleHeapTable *lhs,
                  const storage::ObDirectLoadMultipleHeapTable *rhs);
  int get_error_code() const { return result_code_; }
  int result_code_;
};

class ObTableLoadMultipleHeapTableCompactor : public ObTableLoadTableCompactor
{
  class CompactTaskProcessor;
  class CompactTaskCallback;

public:
  ObTableLoadMultipleHeapTableCompactor();
  virtual ~ObTableLoadMultipleHeapTableCompactor();
  void reset();
  int start() override;
  void stop() override;

  void set_has_error()
  {
    mem_ctx_.has_error_ = true;
  }

private:
  int inner_init() override;
  int construct_compactors();
  int start_compact();
  int start_sort();
  int finish();
  int handle_compact_task_finish(int ret_code);
  int build_result_for_heap_table();
private:
  int add_tablet_table(storage::ObIDirectLoadPartitionTable *table);
  int create_heap_table_sorter(ObDirectLoadMultipleHeapTableSorter *&heap_table_sorter);
private:
  ObTableLoadStoreCtx *store_ctx_;
  const ObTableLoadParam *param_;
  common::ObArenaAllocator allocator_; //需要最后析构
  int64_t finish_task_count_ CACHE_ALIGNED;
  ObDirectLoadMemContext mem_ctx_;
};

} // namespace observer
} // namespace oceanbase
