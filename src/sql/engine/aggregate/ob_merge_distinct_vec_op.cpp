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

#include "objit/common/ob_item_type.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/aggregate/ob_merge_distinct_vec_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

ObMergeDistinctVecSpec::ObMergeDistinctVecSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObDistinctSpec(alloc, type)
{}

OB_SERIALIZE_MEMBER((ObMergeDistinctVecSpec, ObDistinctSpec));

ObMergeDistinctVecOp::ObMergeDistinctVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
    first_got_row_(true),
    alloc_(ObModIds::OB_SQL_MERGE_GROUPBY,
      OB_MALLOC_NORMAL_BLOCK_SIZE, exec_ctx.get_my_session()->get_effective_tenant_id(), ObCtxIds::WORK_AREA),
    last_row_(alloc_)
{
}

int ObMergeDistinctVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (MY_SPEC.is_block_mode_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge distinct not support block mode", K(ret));
  } else if (OB_FAIL(cmp_.init(&eval_ctx_, &MY_SPEC.cmp_funcs_))) {
    LOG_WARN("failed to init cmp", K(ret));
  } else {
    last_row_.reuse_ = true;
  }
  return ret;
}

int ObMergeDistinctVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  first_got_row_ = true;
  last_row_.reset();
  alloc_.reset();
  if (OB_FAIL(ObOperator::inner_rescan())) {
  }
  return ret;
}

int ObMergeDistinctVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = std::min(max_row_cnt, MY_SPEC.max_batch_size_);
  clear_evaluated_flag();
  const ObBatchRows *child_brs = nullptr;
  bool got_batch = false;
  bool has_last = !first_got_row_;
  if (OB_FAIL(child_->get_next_batch(batch_size, child_brs))) {
    LOG_WARN("failed to get next batch", K(ret));
  } else if (child_brs->end_ && 0 == child_brs->size_) {
    ret = OB_ITER_END;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.distinct_exprs_.count(); ++i) {
      if (OB_FAIL(MY_SPEC.distinct_exprs_.at(i)->eval_vector(eval_ctx_, *child_brs))) {
        LOG_WARN("failed to eval batch", K(i), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(deduplicate_for_batch(has_last, child_brs))) {
      LOG_WARN("failed to deduplicate batch", K(ret));
    } else {
      got_batch = true;
    }
  }
  first_got_row_ = false;
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    brs_.end_ = true;
    brs_.size_ = 0;
  }
  return ret;
}

template<typename InputVec>
int ObMergeDistinctVecOp::compare_in_column_with_format(InputVec *vec, const ObBatchRows *child_brs, int64_t first_no_skip_idx,
                                                        int64_t col_idx, int64_t &last_idx) {
  int ret = OB_SUCCESS;
  if (vec->has_null()) {
    if (child_brs->all_rows_active_ && col_idx == 0) {
      if (OB_FAIL((compare_in_column<InputVec, true, true, true>)(vec, first_no_skip_idx, child_brs, col_idx, last_idx))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (child_brs->all_rows_active_ && col_idx != 0) {
      if (OB_FAIL((compare_in_column<InputVec, true, false, true>)(vec, first_no_skip_idx, child_brs, col_idx, last_idx))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (!child_brs->all_rows_active_ && col_idx == 0) {
      if (OB_FAIL((compare_in_column<InputVec, false, true, true>)(vec, first_no_skip_idx, child_brs, col_idx, last_idx))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (!child_brs->all_rows_active_ && col_idx != 0) {
      if (OB_FAIL((compare_in_column<InputVec, false, false, true>)(vec, first_no_skip_idx, child_brs, col_idx, last_idx))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    }
  } else {
    if (child_brs->all_rows_active_ && col_idx == 0) {
      if (OB_FAIL((compare_in_column<InputVec, true, true, false>)(vec, first_no_skip_idx, child_brs, col_idx, last_idx))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (child_brs->all_rows_active_ && col_idx != 0) {
      if (OB_FAIL((compare_in_column<InputVec, true, false, false>)(vec, first_no_skip_idx, child_brs, col_idx, last_idx))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (!child_brs->all_rows_active_ && col_idx == 0) {
      if (OB_FAIL((compare_in_column<InputVec, false, true, false>)(vec, first_no_skip_idx, child_brs, col_idx, last_idx))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (!child_brs->all_rows_active_ && col_idx != 0) {
      if (OB_FAIL((compare_in_column<InputVec, false, false, false>)(vec, first_no_skip_idx, child_brs, col_idx, last_idx))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    }
  }

  return ret;
}

int ObMergeDistinctVecOp::deduplicate_for_batch(bool has_last, const ObBatchRows *child_brs)
{
  int ret = OB_SUCCESS;
  int64_t last_idx = -1;
  int64_t curr_idx = 0;
  int64_t first_no_skip_idx = -1;
  int64_t last_cmp_idx = first_no_skip_idx;
  bool equal = false;

  // 1.get first_no_skip
  brs_.skip_->set_all(child_brs->size_); // default skip
  if (!child_brs->all_rows_active_) {
    while (curr_idx < child_brs->size_) {
      if (!child_brs->skip_->at(curr_idx)) {
        first_no_skip_idx = curr_idx;
        break;
      }
      curr_idx++;
    }
  } else {
    first_no_skip_idx = 0;
  }

  brs_.size_ = child_brs->size_;
  if (has_last) {
    // 2.cmp last and first_no_skip_idx row, let first_no_skip_idx replace the last_row
    if (OB_FAIL(cmp_.equal_in_row(&MY_SPEC.distinct_exprs_, &last_row_, first_no_skip_idx, equal))) {
      LOG_WARN("failed to cmp row", K(ret));
    } else if (equal) {
      // brs_.skip_->set(curr_idx); // no need set, default skip
    } else {
      brs_.skip_->unset(first_no_skip_idx);
      last_idx = first_no_skip_idx;
    }
  } else {
    // no last, out the first_no_skip_idx row
    brs_.skip_->unset(first_no_skip_idx);
    last_idx = first_no_skip_idx;
  }
  if (OB_FAIL(ret)) {
  } else {
    // 3.cmp curr_idx and last_idx col by col
    for (int col_idx = 0; col_idx < MY_SPEC.distinct_exprs_.count() && OB_SUCC(ret); col_idx++){
      bool curr_out = false;
      int cmp_ret;
      ObIVector *vec = MY_SPEC.distinct_exprs_.at(col_idx)->get_vector(eval_ctx_);
      last_cmp_idx = first_no_skip_idx;
      curr_idx = first_no_skip_idx + 1;
      switch (vec->get_format()) {
        case VEC_FIXED : {
          // 对big_int进行特化
          if (ob_is_integer_type(MY_SPEC.distinct_exprs_.at(col_idx)->datum_meta_.type_)) {
            // 测试发现，对于bigint类型，当前列的每一行不判断（是否被上一列的比较输出）直接进行比较性能通常更高(at有位运算导致)，
            // 首列过滤行较少时，将后续列看作是first_col不进行当前行out的取值性能会更好
            // 而对于第一列就输出大量行，此时仍将后续列视为first_col的话性能会发生较大回退，因此暂时不进行该优化
            ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>> *fixed_vec =
              static_cast<ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>> *> (vec);
            compare_in_column_with_format<FixedLengthVectorBigInt>(fixed_vec, child_brs, first_no_skip_idx, col_idx, last_idx);
          } else {
            compare_in_column_with_format<ObIVector>(vec, child_brs, first_no_skip_idx, col_idx, last_idx);
          }
          break;
        }
        case VEC_DISCRETE : {
          if (ob_is_string_tc(MY_SPEC.distinct_exprs_.at(col_idx)->datum_meta_.type_)) {
            ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> *string_vec = static_cast<ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> *> (vec);
            compare_in_column_with_format<DiscreteVectorString>(string_vec, child_brs, first_no_skip_idx, col_idx, last_idx);
          } else {
            compare_in_column_with_format<ObIVector>(vec, child_brs, first_no_skip_idx, col_idx, last_idx);
          }
          break;
        }
        default : {
          compare_in_column_with_format<ObIVector>(vec, child_brs, first_no_skip_idx, col_idx, last_idx);
        }
      }
      if (brs_.skip_->accumulate_bit_cnt(child_brs->size_) <= first_no_skip_idx) {
        // 说明brs_.skip_从first_no_skip_idx开始往下全部为false,全部被输出了,不需要再比较下一列
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    // 4.store the last row
    if (last_idx != -1) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
      batch_info_guard.set_batch_idx(last_idx);
      if (OB_FAIL(last_row_.save_store_row(MY_SPEC.distinct_exprs_, *child_brs, eval_ctx_, 0))) {
        LOG_WARN("failed to save last row");
      }
    }
  }
  return ret;
}

template<typename InputVec, bool ALL_ROWS_ACTIVE, bool FIRST_COL, bool HAS_NULL>
int ObMergeDistinctVecOp::compare_in_column(InputVec * vec, int64_t first_no_skip_idx,
                                            const ObBatchRows *child_brs, int64_t col_idx, int64_t &last_idx) {
  int ret = OB_SUCCESS;
  int null_type = 0;
  int cmp_ret = 0;
  int64_t last_cmp_idx = first_no_skip_idx;
  int64_t curr_idx = first_no_skip_idx + 1;
  const sql::ObExpr &col_expr = *MY_SPEC.distinct_exprs_.at(col_idx);
  for (; curr_idx < child_brs->size_ && OB_SUCC(ret) ; curr_idx++) {
    if (ALL_ROWS_ACTIVE && FIRST_COL) { // skip and out are false, do not continue, need compare
    } else if (ALL_ROWS_ACTIVE && !FIRST_COL) { // skip is false, judge out
      if (!brs_.skip_->at(curr_idx)) {
        // curr_idx row is out, do not need compare, but need update last_cmp_idx
        last_cmp_idx = curr_idx;
        continue;
      }
    } else if (!ALL_ROWS_ACTIVE && FIRST_COL) { // out is false, judge skip
      if (child_brs->skip_->at(curr_idx)) { continue; } // skip row
    } else if (!ALL_ROWS_ACTIVE && !FIRST_COL) {
      if (child_brs->skip_->at(curr_idx)) { continue; }
      if (!brs_.skip_->at(curr_idx)) {
        last_cmp_idx = curr_idx;
        continue;
      }
    }

    // curr_idx row is neither skip nor out, cmp with last_cmp_idx
    if (HAS_NULL) {
      if (OB_FAIL(vec->null_last_cmp(col_expr, curr_idx, vec->is_null(last_cmp_idx),
                                    vec->get_payload(last_cmp_idx), vec->get_length(last_cmp_idx), cmp_ret))) {
        LOG_WARN("null_last_cmp failed", K(curr_idx), K(last_cmp_idx), K(cmp_ret), K(ret));
      }
    } else {
      if (OB_FAIL(vec->no_null_cmp(col_expr, curr_idx, last_cmp_idx, cmp_ret))) {
        LOG_WARN("no_null_cmp failed", K(curr_idx), K(last_cmp_idx), K(cmp_ret), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (cmp_ret == 0) { // equal, do nothing
    } else {
      brs_.skip_->unset(curr_idx);
      if (last_idx < curr_idx) {
        last_idx = curr_idx;
      }
    }
    last_cmp_idx = curr_idx;
  }
  return ret;
}

void ObMergeDistinctVecOp::destroy()
{
  last_row_.reset();
  alloc_.reset();
  ObOperator::destroy();
}

int ObMergeDistinctVecOp::inner_close()
{
  int ret = OB_SUCCESS;
  last_row_.reset();
  alloc_.reset();
  if (OB_FAIL(ObOperator::inner_close())) {
  }
  return ret;
}

int ObMergeDistinctVecOp::Compare::init(ObEvalCtx *eval_ctx, const ObIArray<ObCmpFunc> *cmp_funcs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx) || OB_ISNULL(cmp_funcs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to init compare funcs", K(ret));
  } else {
    eval_ctx_ = eval_ctx;
    cmp_funcs_ = cmp_funcs;
    ret_code_ = OB_SUCCESS;
  }
  return ret;
}

int ObMergeDistinctVecOp::Compare::equal_in_row(const common::ObIArray<ObExpr*> *set_exprs,
                                                const LastCompactRow *r,
                                                const int64_t curr_idx,
                                                bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  if (0 == set_exprs->count()) {
    // 表示是distinct 常量，所以没有distinct列，则永远相等
    // case: select distinct 1 from t1;
    equal = true;
  } else {
    int cmp = 0;
    const char *r_v = NULL;
    ObLength r_len = 0;
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < set_exprs->count(); i++) {
      ObIVector *vec = set_exprs->at(i)->get_vector(*eval_ctx_);
      r->compact_row_->get_cell_payload(r->row_meta_, i, r_v, r_len);
      if (OB_FAIL(vec->null_last_cmp(*set_exprs->at(i), curr_idx, r->compact_row_->is_null(i),
                                      r_v, r_len, cmp))) {
        LOG_WARN("failed to cmp left and right", K(ret));
      }
    }
    equal = (0 == cmp);
  }
  return ret;
}
} // end namespace sql
} // end namespace oceanbase
