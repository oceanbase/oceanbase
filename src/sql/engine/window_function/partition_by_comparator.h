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

#ifndef OCEANBASE_PARTITION_BY_COMPARATOR_H_
#define OCEANBASE_PARTITION_BY_COMPARATOR_H_

#include "lib/container/ob_se_array.h"
#include "lib/list/ob_dlist.h"
#include "win_expr.h"

namespace oceanbase
{
namespace sql
{
namespace winfunc {
struct WinFuncPartitionByInfo {
  WinFuncPartitionByInfo()
      : pby_row_mapped_idxes_(nullptr), reordered_pby_row_idx_(nullptr) {}
  // current partition key(encoded), 1 row
  int32_t *pby_row_mapped_idxes_;
  int32_t *reordered_pby_row_idx_;
};

class PartitionByComparator {
public:
  PartitionByComparator()
      : max_pby_col_cnt_(0), pby_row_mapped_idx_arr_(nullptr),
        last_row_idx_arr_(nullptr), eval_ctx_(nullptr), all_exprs_(nullptr),
        input_row_meta_(nullptr), all_exprs_backup_buf_(nullptr), all_exprs_backup_buf_len_(0), eval_infos_(nullptr) {}

  using WinFuncColExprList = common::ObDList<WinFuncColExpr>;
  using WFInfoFixedArray = common::ObFixedArray<WinFuncInfo, common::ObIAllocator>;
  int init(const ObMemAttr &attr, const WFInfoFixedArray &wf_infos,
           ObArenaAllocator *local_allocator, int64_t max_batch_size,
           WinFuncColExprList &wf_list, ObEvalCtx *eval_ctx,
           ObExprPtrIArray *all_expr, RowMeta *input_row_meta,
           char* all_exprs_backup_buf, int32_t all_exprs_backup_buf_len, const ObCompactRow **stored_rows,
           common::ObFixedArray<ObEvalInfo *, common::ObIAllocator>* eval_infos);

  void reset() {
    all_part_exprs_.reset();
  }

  // write pby_row_mapped_idx_arr_
  int mapping_pby_row_to_idx_arr(const ObBatchRows &child_brs,
                                 const ObCompactRow *last_row,
                                 const int64_t tenant_id);
  int check_same_partition(WinFuncColExpr &wf_col, bool &same);
  int find_same_partition_of_wf(WinFuncColExprList& wf_infos, WinFuncColExpr *&result);
  // copy from this.pby_row_mapped_idx_arr_ to the pby_row_mapped_idxes_ of the wf_cols
  int save_pby_row_for_wf(WinFuncColExpr* start, WinFuncColExpr *end_wf,
                                    const int64_t batch_idx);
  int64_t get_max_pby_col_cnt() const { return max_pby_col_cnt_; }

private:
  struct cell_info {
    bool is_null_;
    int32_t len_;
    const char *payload_;
    cell_info(bool is_null, int32_t len, const char *payload)
        : is_null_(is_null), len_(len), payload_(payload) {}
    cell_info() : is_null_(true), len_(0), payload_(nullptr) {}
    TO_STRING_KV(K_(is_null), K_(len), KP_(payload));
  };
  template <typename ColumnFormat>
  int mapping_pby_col_to_idx_arr(int32_t col_id, const ObExpr &part_expr,
                                 const ObBatchRows &brs,
                                 const cell_info *last_part_res);
  int eval_prev_part_exprs(const ObCompactRow *last_row, ObIAllocator &alloc,
                           const ObExprPtrIArray &part_exprs,
                           common::ObIArray<cell_info> &last_part_infos);

  // same as ObOperator::clear_evaluated_flag()
  void clear_evaluated_flag();

  int64_t max_pby_col_cnt_;
  // current batch's partition key(encoded) for all rows
  int32_t *pby_row_mapped_idx_arr_;
  int32_t *last_row_idx_arr_;
  ObSEArray<ObExpr *, 8> all_part_exprs_;

  // shallow copy: used for evaluating partition expressions
  ObEvalCtx *eval_ctx_;
  ObExprPtrIArray *all_exprs_;
  RowMeta *input_row_meta_;
  char* all_exprs_backup_buf_;
  int32_t all_exprs_backup_buf_len_;
  const ObCompactRow** stored_rows_;
  // after evaluate partition expressions, we need to clear evaluated flags
  common::ObFixedArray<ObEvalInfo *, common::ObIAllocator>* eval_infos_;
};


} // namespace winfunc
} // namespace sql
} // namespace oceanbase

#endif
