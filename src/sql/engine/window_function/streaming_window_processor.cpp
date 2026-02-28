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

#define USING_LOG_PREFIX SQL_ENG
#include "streaming_window_processor.h"
#include "ob_window_function_vec_op.h"

namespace oceanbase {
namespace sql {
using winfunc::IWinExpr;

int StreamingWindowProcessor::init(ObEvalCtx *eval_ctx, RowMeta *input_row_meta,
                                   ObExprPtrIArray *input_exprs,
                                   WinFuncColExprList &wf_list,
                                   PartitionByComparator *pby_comparator,
                                   ExprFixedArray *rd_coord_exprs) {
  int ret = OB_SUCCESS;
  if (!OB_ISNULL(input_exprs_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(eval_ctx) || OB_ISNULL(pby_comparator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(eval_ctx), K(pby_comparator));
  } else {
    eval_ctx_ = eval_ctx;
    input_row_meta_ = input_row_meta;
    input_exprs_ = input_exprs;
    rd_coord_exprs_ = rd_coord_exprs;
    wf_list_ = &wf_list;
    pby_comparator_ = pby_comparator;
    for (WinFuncColExpr *it = wf_list.get_first(); it != wf_list.get_header(); it = it->get_next()) {
      if (OB_FAIL(streaming_prev_row_idxes_.push_back(-1))) {
        LOG_WARN("failed to init streaming prev row idxes", K(ret));
      }
    }
  }
  return ret;
}

void StreamingWindowProcessor::reset() {
  LOG_TRACE("reset streaming window processor", K(metrics_));
  per_batch_allocator_.reset();
  last_row_ = nullptr;
  rd_last_row_ = nullptr;
  is_first_batch_ = true;
}

int StreamingWindowProcessor::process_next_batch(const ObBatchRows &child_brs,
                                                 ObBatchRows &output) {
  int ret = OB_SUCCESS;
  int64_t last_row_index = -1;

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(on_batch_start(child_brs))) {
    LOG_WARN("pre process batch failed", K(ret), K(metrics_));
  } else {
    bool all_in_same_partition = false;
    if (OB_FAIL(check_all_in_same_partition(child_brs, all_in_same_partition,
                                            last_row_index))) {
      LOG_WARN("check all in same partition failed", K(ret), K(metrics_));
    } else if (all_in_same_partition) {
      // fast path: all in the same partition, just evaluate all the window
      // functions of all rows
      if (OB_FAIL(compute_all_wf_values(child_brs, 0, child_brs.size_))) {
        LOG_WARN("compute wf values of same partition failed", K(ret),
                 K(metrics_));
      }
    } else {
      // find partition end + evaluate window functions of each partition
      if (child_brs.all_rows_active_) {
        if (OB_FAIL(process_batch_of_various_partitions<false>(child_brs))) {
          LOG_WARN("process batch of various partitions failed", K(ret),
                   K(metrics_));
        }
      } else {
        if (OB_FAIL(process_batch_of_various_partitions<true>(child_brs))) {
          LOG_WARN("process batch of various partitions failed", K(ret),
                   K(metrics_));
        }
      }
    }
  }

  if (OB_SUCC(ret) &&
      OB_FAIL(on_batch_end(child_brs, last_row_index, output))) {
    LOG_WARN("batch cleanup failed", K(ret), K(metrics_));
  }
  metrics_.num_batches_ += 1;
  LOG_TRACE("process next batch", K(ret), K(metrics_), K(output));
  return ret;
}

int StreamingWindowProcessor::on_batch_start(const ObBatchRows &child_brs) {
  int ret = OB_SUCCESS;
  if (is_first_batch_) {
    if (OB_FAIL(on_partition_start(wf_list_->get_header()))) {
      LOG_WARN("explicitly start partition if needed failed", K(ret));
    }
    is_first_batch_ = false;
  }

  // rank() and dense_rank() rely on last row of previous batch
  for (WinFuncColExpr *it = wf_list_->get_first();
       OB_SUCC(ret) && it != wf_list_->get_header(); it = it->get_next()) {
    winfunc::StreamingWinExprEvalCtx ctx(*it, *input_exprs_, *eval_ctx_,
                                         child_brs, per_batch_allocator_);
    if (OB_FAIL(winfunc::StreamingWinExprEvalCtx::on_batch_start_with_last_row(
            ctx, last_row_, child_brs))) {
      LOG_WARN("failed to on batch start with last row", K(ret), K(*it),
               K(last_row_), K(child_brs));
    }
  }

  // reuse per-batch allocator, now last_row_ is invalid
  per_batch_allocator_.reuse();
  last_row_ = nullptr;
  // update previous row idxes to -1 for all window functions
  for (WinFuncColExpr *it = wf_list_->get_first(); it != wf_list_->get_header(); it = it->get_next()) {
    streaming_prev_row_idxes_.at(it->wf_idx_ - 1) = -1;
  }

  return ret;
}

// check if all in the same partition from backward
int StreamingWindowProcessor::check_all_in_same_partition(
    const ObBatchRows &child_brs, bool /* out */ &all_in_same_partition,
    int64_t & /*out */ last_row_index) {
  int ret = OB_SUCCESS;
  last_row_index = -1;
  for (int i = child_brs.size_ - 1; i >= 0; i--) {
    if (child_brs.skip_->at(i)) {
      continue;
    }
    last_row_index = i;
    break;
  }
  if (last_row_index < 0) {
    ret = OB_UNEXPECT_INTERNAL_ERROR;
    LOG_WARN("unexpected internal error: no valid row in child_brs", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx_);
    guard.set_batch_size(child_brs.size_);
    guard.set_batch_idx(last_row_index);
    if (OB_FAIL(pby_comparator_->check_same_partition(*wf_list_->get_first(),
                                                      all_in_same_partition))) {
      LOG_WARN("check same partition failed", K(ret));
    }
  }
  return ret;
}

int StreamingWindowProcessor::on_partition_start(WinFuncColExpr *end) {
  int ret = OB_SUCCESS;
  if (end == wf_list_->get_header()) {
    // a big partition, reset last_computed_part_rows_
    last_computed_part_rows_ = 0;
  }

  for (WinFuncColExpr *it = wf_list_->get_first(); OB_SUCC(ret) && it != end;
       it = it->get_next()) {
    if (OB_FAIL(it->on_partition_start())) {
      LOG_WARN("reset for new partition failed", K(ret));
    } else {
      metrics_.num_partitions_++;
      streaming_prev_row_idxes_.at(it->wf_idx_ - 1) = -1;
    }
  }
  return ret;
}

int StreamingWindowProcessor::compute_all_wf_values(
    const ObBatchRows &child_brs, int64_t start_idx, int64_t end_idx) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_idx == end_idx)) {
    // the first partition, do nothing
  } else {
    for (WinFuncColExpr *it = wf_list_->get_first();
         OB_SUCC(ret) && it != wf_list_->get_header(); it = it->get_next()) {
      winfunc::StreamingWinExprEvalCtx ctx(*it, *input_exprs_, *eval_ctx_,
                                           child_brs, per_batch_allocator_);
      int64_t prev_row_idx = streaming_prev_row_idxes_.at(it->wf_idx_ - 1);
      if (OB_FAIL(it->wf_expr_->process_rows_streaming(ctx, prev_row_idx, start_idx, end_idx,
                                                       child_brs))) {
        LOG_WARN("failed to streamingly process in row mode", K(ret), K(*it),
                 K(start_idx), K(end_idx));
      } else if (OB_FAIL(it->wf_expr_->collect_part_results(
                     ctx.win_col_, start_idx, end_idx, *child_brs.skip_,
                     false))) {
        LOG_WARN("collect part results failed", K(ret));
      }
    }
  }

  // increment last_computed_part_rows_ by the number of rows in the current
  // partition
  if (child_brs.all_rows_active_) {
    metrics_.num_rows_ += end_idx - start_idx;
    last_computed_part_rows_ += end_idx - start_idx;
  } else {
    for (int i = start_idx; i < end_idx; i++) {
      if (child_brs.skip_->at(i)) {
        continue;
      }
      metrics_.num_rows_++;
      last_computed_part_rows_++;
    }
  }
  return ret;
}

template <bool not_all_rows_active>
int StreamingWindowProcessor::process_batch_of_various_partitions(
    const ObBatchRows &child_brs) {
  int ret = OB_SUCCESS;
  WinFuncColExpr *first = wf_list_->get_first();
  WinFuncColExpr *end = nullptr;
  int64_t start_idx = 0;
  int64_t batch_size = child_brs.size_;
  ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx_);
  guard.set_batch_size(batch_size);
  int p = -1; // previous valid row index
  for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
    bool same_part = true;
    if constexpr (not_all_rows_active) {
      if (child_brs.skip_->at(i)) {
        continue;
      }
    }
    guard.set_batch_idx(i);

    if (OB_FAIL(pby_comparator_->check_same_partition(*first, same_part))) {
      LOG_WARN("check same partition failed", K(ret));
    } else if (OB_UNLIKELY(!same_part)) {
      // found a new partition
      if (OB_FAIL(pby_comparator_->find_same_partition_of_wf(*wf_list_, end))) {
        LOG_WARN("find same partition of wf failed", K(ret));
      } else if (OB_FAIL(pby_comparator_->save_pby_row_for_wf(first, end, i))) {
        LOG_WARN("save partition groupby row failed", K(ret));
      } else {
        // compute wf values of all the window function on previous_partition
        if (OB_FAIL(compute_all_wf_values(child_brs, start_idx, i))) {
          LOG_WARN("compute wf values of same partition failed", K(ret));
        } else if (OB_FAIL(on_partition_start(end))) {
          LOG_WARN("reset for new partition failed", K(ret));
        } else {
          // update streaming prev row idxes(that are not encountered a new partition)
          for (WinFuncColExpr *it = end; it != wf_list_->get_header(); it = it->get_next()) {
            streaming_prev_row_idxes_.at(it->wf_idx_ - 1) = p;
          }
        }
        start_idx = i;
      }
    } // if unlikely not in the same partition
    p = i;
  }   // end for i in row

  // compute the last partition
  if (OB_SUCC(ret) &&
      OB_FAIL(compute_all_wf_values(child_brs, start_idx, batch_size))) {
    LOG_WARN("compute wf values of last partition failed", K(ret), K(start_idx),
             K(batch_size));
  }
  return ret;
}

int StreamingWindowProcessor::on_batch_end(const ObBatchRows &child_brs,
                                           const int64_t last_row_index,
                                           ObBatchRows &output) {
  int ret = OB_SUCCESS;
  for (WinFuncColExpr *it = wf_list_->get_first();
       OB_SUCC(ret) && it != wf_list_->get_header(); it = it->get_next()) {
    it->wf_info_.expr_->set_evaluated_projected(*eval_ctx_);
    winfunc::StreamingWinExprEvalCtx ctx(*it, *input_exprs_, *eval_ctx_,
                                         child_brs, per_batch_allocator_);
    if (OB_FAIL(winfunc::StreamingWinExprEvalCtx::on_batch_end(
            ctx, last_row_index))) {
      LOG_WARN("failed to on batch end", K(ret), K(*it), K(last_row_index),
               K(child_brs));
    }
  }

  if (OB_SUCC(ret)) {
    // evaluate last row as ObCompactRow
    output.skip_->deep_copy(*child_brs.skip_, child_brs.size_);
    output.size_ = child_brs.size_;
    output.end_ = child_brs.end_;
    output.all_rows_active_ = child_brs.all_rows_active_;

    // save last row to agg function + partition by comparator
    LastCompactRow last_compact_row(per_batch_allocator_);
    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx_);
    guard.set_batch_idx(last_row_index);
    if (OB_FAIL(last_compact_row.save_store_row(
            *input_exprs_, output, *eval_ctx_, *input_row_meta_))) {
      LOG_WARN("save store row failed", K(ret), K(last_row_index));
    } else {
      last_row_ = last_compact_row.compact_row_;
    }

    // save last row to range distribution optimization
    if (rd_coord_exprs_ != nullptr && OB_SUCC(ret)) {
      LastCompactRow rd_last_compact_row(per_batch_allocator_);
      if (OB_FAIL(rd_last_compact_row.save_store_row(
              *rd_coord_exprs_, output, *eval_ctx_, sizeof(int64_t), false))) {
        LOG_WARN("save store row failed", K(ret), K(last_row_index));
      } else {
        rd_last_row_ = rd_last_compact_row.compact_row_;
      }
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
