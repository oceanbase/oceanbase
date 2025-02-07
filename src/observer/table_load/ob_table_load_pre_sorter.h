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
#ifndef _OB_TABLE_LOAD_PRE_SORT_H_
#define _OB_TABLE_LOAD_PRE_SORT_H_
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_parallel_merge_ctx.h"
#include "observer/table_load/ob_table_load_table_compactor.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_mem_context.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"
#include "observer/table_load/ob_table_load_mem_chunk_manager.h"

namespace oceanbase
{
namespace observer
{
  class ObTableLoadTableCtx;
} /* namespace observer */
namespace observer
{
class ObTableLoadPreSorter
{
public:
  using ObTableLoadTablePreSortResult = ObTableLoadTableCompactResult;
  ObTableLoadPreSorter(ObTableLoadTableCtx *ctx,
                       ObTableLoadStoreCtx *store_ctx);
  ~ObTableLoadPreSorter();
  int init();
  int start();
  int set_all_trans_finished();
  void stop();
  void set_has_error() { mem_ctx_.has_error_ = true; }
private:
  int init_task_count();
  int init_mem_ctx();
  int init_sample_task_scheduler();
  int init_chunks_manager();
  int start_sample();
  int start_dump();
  int handle_pre_sort_task_finish(int ret_code);
  int prepare_parallel_merge(); // dump 结束之后开启parallel_merge工作
  int add_table_to_parallel_merge_ctx();
  int start_parallel_merge();
  int handle_parallel_merge_success();
  int build_merge_param(ObDirectLoadMergeParam& merge_param);
  int build_merge_ctx_for_multiple_mode(ObDirectLoadMergeParam& merge_param,
                                        ObTableLoadMerger *merger,
                                        ObTableLoadTableCompactResult &result);
  int build_merge_ctx_for_non_multiple_mode(ObDirectLoadMergeParam& merge_param,
                                            ObTableLoadMerger *merger,
                                            ObTableLoadTableCompactResult &result);
  int build_parallel_merge_result();
  int handle_pre_sort_success();
  int finish();
  int build_merge_ctx();
  int start_merge();
private:
  class PreSortSampleTaskProcessor;
  class PreSortDumpTaskProcessor;
  class PreSortParallelMergeTaskProcessor;
  class PreSortFinishTaskProcessor;
private:
  class PreSortTaskCallback;
  class PreSortParallelMergeTaskCallback;
  class PreSortFinishTaskCallback;
public:
  int64_t finish_task_count_;
  int64_t finish_write_task_count_;
  int64_t running_write_task_count_;
  ObDirectLoadMemContext mem_ctx_;
  ObTableLoadMemChunkManager *chunks_manager_;
  observer::ObTableLoadTableCtx *ctx_;
  observer::ObTableLoadStoreCtx *store_ctx_;
  int32_t other_task_count_;
  int32_t dump_task_count_;
private:
  class ParallelMergeCb : public ObTableLoadParallelMergeCb
  {
  public:
    ParallelMergeCb(ObTableLoadPreSorter *pre_sorter_) : pre_sorter_(pre_sorter_) {}
    int on_success() override;
  private:
    ObTableLoadPreSorter *pre_sorter_;
  };
private:
  ObITableLoadTaskScheduler *sample_task_scheduler_;
  ObTableLoadParallelMergeCtx parallel_merge_ctx_;
  ParallelMergeCb parallel_merge_cb_;
  bool all_trans_finished_;
  bool is_inited_;
  bool is_start_;
};
} /* namespace storage */
} /* namespace oceanbase */

#endif /*_OB_TABLE_LOAD_PRE_SORT_H_*/