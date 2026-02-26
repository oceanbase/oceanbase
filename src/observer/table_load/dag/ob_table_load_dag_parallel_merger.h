/**
 * Copyright (c) 2025 OceanBase
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

#include "storage/direct_load/ob_direct_load_merge_ctx.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadTableOpCtx;

class ObTableLoadDagParallelMerger
{
public:
  ObTableLoadDagParallelMerger();
  ~ObTableLoadDagParallelMerger();
  int init_merge_task(ObTableLoadStoreCtx *store_ctx, ObTableLoadTableOpCtx *op_ctx);
  int get_next_merge_task(ObDirectLoadIMergeTask *&merge_task);
  ObDirectLoadMergeCtx &get_merge_ctx() { return merge_ctx_; }
  int prepare_clear_table();
  int clear_table(const int64_t thread_cnt, const int64_t thread_idx);
  TO_STRING_KV(K(task_iter_), K(is_inited_));

private:
  int init_merge_ctx();

private:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadTableOpCtx *op_ctx_;
  storage::ObDirectLoadMergeCtx merge_ctx_;
  mutable lib::ObMutex mutex_;
  ObDirectLoadMergeTaskIterator task_iter_;
  ObDirectLoadTableHandleArray all_table_handles_;
  bool clear_table_prepared_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
