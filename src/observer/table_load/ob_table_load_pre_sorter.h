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
class ObDirectLoadTableStore;
} // namespace storage
namespace observer
{
class ObTableLoadTableCtx;
class ObTableLoadStoreCtx;
class ObTableLoadMemChunkManager;
class ObITableLoadTaskScheduler;

class ObTableLoadPreSorter
{
  class SampleTaskProcessor;
  class DumpTaskProcessor;
  class PreSortTaskCallback;
  class FinishTaskProcessor;
  class FinishTaskCallback;
  class ChunkSorter;

  friend class ObTableLoadPreSortWriter;
public:
  ObTableLoadPreSorter(ObTableLoadStoreCtx *store_ctx);
  ~ObTableLoadPreSorter();
  void reset();
  int init();
  int start();
  int close();
  void stop();
  void wait();
  bool is_stopped() const;
  void set_has_error() { mem_ctx_.has_error_ = true; }
  int get_table_store(ObDirectLoadTableStore &table_store);
public:
  OB_INLINE int64_t inc_sort_chunk_task_cnt() { return ATOMIC_AAF(&sort_chunk_task_cnt_, 1); }
  OB_INLINE int64_t dec_sort_chunk_task_cnt() { return ATOMIC_AAF(&sort_chunk_task_cnt_, -1); }
  OB_INLINE int64_t get_sort_chunk_task_cnt() { return ATOMIC_LOAD(&sort_chunk_task_cnt_); }
private:
  int init_mem_ctx();
  int init_chunks_manager();
  int init_sample_task_scheduler();
  int start_sample();
  int start_dump();
  int start_finish();
  int close_chunk(int64_t chunk_node_id);
  int handle_pre_sort_thread_finish();
  int finish();
private:
  ObTableLoadTableCtx *ctx_;
  ObTableLoadStoreCtx *store_ctx_;
  ObArenaAllocator allocator_;
  ObDirectLoadMemContext mem_ctx_;
  ObTableLoadMemChunkManager *chunks_manager_;
  ObITableLoadTaskScheduler *sample_task_scheduler_;
  int64_t unclosed_chunk_id_pos_;
  int64_t finish_thread_cnt_;
  int64_t sort_chunk_task_cnt_;
  bool all_trans_finished_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
