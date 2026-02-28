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

#ifndef OCEANBASE_STREAMING_WINDOW_PROCESSOR_H_
#define OCEANBASE_STREAMING_WINDOW_PROCESSOR_H_

#include "partition_by_comparator.h"
#include "sql/engine/basic/ob_temp_column_store.h"
#include "win_expr.h"

namespace oceanbase {
namespace sql {

namespace winfunc {
class PartitionByComparator;
} // namespace winfunc

class StreamingWindowProcessor {
private:
  struct Metrics {
    int64_t num_batches_ = 0;
    int64_t num_rows_ = 0;
    int64_t num_partitions_ = 0;
    TO_STRING_KV(K(num_batches_), K(num_rows_), K(num_partitions_));
  };
  Metrics metrics_;

public:
  using WinFuncColExprList = common::ObDList<WinFuncColExpr>;
  using PartitionByComparator = winfunc::PartitionByComparator;
  StreamingWindowProcessor(int64_t tenant_id)
      : last_computed_part_rows_(0), input_exprs_(nullptr),
        input_row_meta_(nullptr), wf_list_(nullptr), pby_comparator_(nullptr),
        per_batch_allocator_(ObModIds::OB_SQL_WINDOW_LOCAL,
                             OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id,
                             ObCtxIds::WORK_AREA),
        last_row_(nullptr), rd_last_row_(nullptr), is_first_batch_(true) {}

  ~StreamingWindowProcessor() { reset(); }

  int init(ObEvalCtx *eval_ctx, RowMeta *input_row_meta,
           ObExprPtrIArray *input_exprs, WinFuncColExprList &win_col_expr_list,
           PartitionByComparator *pby_comparator,
           ExprFixedArray *rd_coord_exprs);

  void reset();

  int process_next_batch(const ObBatchRows &child_brs, ObBatchRows &output);

  ObCompactRow *get_last_row() const { return last_row_; }

  ObCompactRow *get_rd_last_row() const { return rd_last_row_; }

  int get_last_computed_part_rows() const { return last_computed_part_rows_; }

  bool has_processed_rows() const { return metrics_.num_rows_ > 0; }

  // called in streaming window processor in range distribution optimization
  // for the second partition
  int on_partition_start(WinFuncColExpr *end);

private:
  // pre-process the batch before the current batch is processed
  int on_batch_start(const ObBatchRows &child_brs);
  int check_all_in_same_partition(const ObBatchRows &child_brs,
                                  bool & /*out*/ all_in_same_partition,
                                  int64_t & /*out*/ last_row_index);

  template <bool is_all_rows_active>
  int process_batch_of_various_partitions(const ObBatchRows &child_brs);
  int compute_all_wf_values(const ObBatchRows &child_brs, int64_t start_idx,
                            int64_t end_idx);

  // post-process the batch after the current batch is processed
  int on_batch_end(const ObBatchRows &child_brs, const int64_t last_row_index,
                   ObBatchRows &output);

private:
  // used in range distribution
  // number of rows in last partition (of last/max window function)
  int64_t last_computed_part_rows_;

  // set pointer in init() once and for all
  ObExprPtrIArray *input_exprs_;
  RowMeta *input_row_meta_;
  ObEvalCtx *eval_ctx_;
  WinFuncColExprList *wf_list_;
  PartitionByComparator *pby_comparator_;
  // exprs for range distribution
  ExprFixedArray *rd_coord_exprs_;
  // end

  // reuse on every batch
  common::ObArenaAllocator per_batch_allocator_;
  ObCompactRow *last_row_;
  // for range distribution
  ObCompactRow *rd_last_row_;

  // mark first batch + will call on_partition_start()
  // set to true in reset()/rescan()
  bool is_first_batch_;
  // streaming processing will use accumulated previous row index to compute wf values
  // the value of the hash map is the preivious row index of the last computed batch
  ObSEArray<int64_t, 10> streaming_prev_row_idxes_;
};

} // namespace sql
} // namespace oceanbase

#endif