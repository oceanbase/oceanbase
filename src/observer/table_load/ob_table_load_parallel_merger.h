/**
 * Copyright (c) 2024 OceanBase
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

#include "storage/direct_load/ob_direct_load_i_merge_task.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadMergeTableBaseOp;

class ObTableLoadParallelMerger
{
  class MergeTaskProcessor;
  class MergeTaskCallback;

public:
  ObTableLoadParallelMerger();
  ~ObTableLoadParallelMerger();
  int init_merge_task(ObTableLoadMergeTableBaseOp *op);
  int init_rescan_task(ObTableLoadMergeTableBaseOp *op);
  int start();
  void stop();
  ObDirectLoadMergeCtx &get_merge_ctx() { return merge_ctx_; }

private:
  int init_merge_ctx(ObTableLoadMergeTableBaseOp *op);

  int get_next_merge_task(ObDirectLoadIMergeTask *&merge_task);
  int handle_merge_task_finish(ObDirectLoadIMergeTask *merge_task, int ret_code);
  int handle_merge_thread_finish(int ret_code);

private:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadMergeTableBaseOp *op_;
  ObDirectLoadMergeCtx merge_ctx_;
  mutable lib::ObMutex mutex_;
  ObDirectLoadMergeTaskIterator task_iter_;
  common::ObDList<ObDirectLoadIMergeTask> running_task_list_;
  int64_t running_thread_cnt_ CACHE_ALIGNED;
  bool has_error_;
  bool is_stop_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
