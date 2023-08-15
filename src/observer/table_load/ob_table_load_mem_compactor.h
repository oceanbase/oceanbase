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
#include "observer/table_load/ob_table_load_parallel_merge_ctx.h"

namespace oceanbase
{
namespace storage
{
  class ObDirectLoadMemDump;
  class ObDirectLoadMemLoader;
}

namespace observer
{
class ObTableLoadParam;
class ObITableLoadTaskScheduler;

class ObTableLoadMemCompactor : public ObTableLoadTableCompactor
{
  class CompactTaskProcessor;
  class CompactTaskCallback;
  class SampleTaskProcessor;
  class MemDumpTaskProcessor;
  class MemDumpTaskCallback;
  class FinishTaskProcessor;
  class FinishTaskCallback;

public:
  ObTableLoadMemCompactor();
  virtual ~ObTableLoadMemCompactor();
  void reset();
  int start() override;
  void stop() override;

  void set_has_error()
  {
    mem_ctx_.has_error_ = true;
  }

private:
  class ParallelMergeCb : public ObTableLoadParallelMergeCb
  {
  public:
    ParallelMergeCb(ObTableLoadMemCompactor *compactor) : compactor_(compactor) {}
    int on_success() override;
  private:
    ObTableLoadMemCompactor *compactor_;
  };

private:
  int inner_init() override;
  int construct_compactors();
  int start_compact();
  int start_load();
  int start_sample();
  int start_dump();
  int start_finish();
  int handle_compact_task_finish(int ret_code);
  int handle_merge_success();
  int build_result();
  int add_table_to_parallel_merge_ctx();
  int init_scheduler();
  int finish();
  int start_parallel_merge();
  int build_result_for_heap_table();
private:
  int add_tablet_table(storage::ObIDirectLoadPartitionTable *table);
  int create_mem_loader(ObDirectLoadMemLoader *&mem_loader);
  int64_t get_compact_task_count() const;
private:
  ObTableLoadStoreCtx *store_ctx_;
  const ObTableLoadParam *param_;
  common::ObArenaAllocator allocator_; //需要最后析构
  int64_t finish_task_count_ CACHE_ALIGNED;
  ObITableLoadTaskScheduler *task_scheduler_;
  ObDirectLoadMemContext mem_ctx_;
  ObTableLoadParallelMergeCtx parallel_merge_ctx_;
  ParallelMergeCb parallel_merge_cb_;
};

} // namespace observer
} // namespace oceanbase
