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

#include "ob_limit_op.h"
#include "ob_limit_vec_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_material_vec_op.h"
#include "sql/engine/sort/ob_sort_op.h"
#include "sql/engine/sort/ob_sort_vec_op.h"
#include "sql/engine/basic/ob_material_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObLimitVecSpec::ObLimitVecSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      limit_expr_(NULL),
      offset_expr_(NULL),
      percent_expr_(NULL),
      calc_found_rows_(false),
      is_top_limit_(false),
      is_fetch_with_ties_(false),
      sort_columns_(alloc)
{
}

ObLimitVecSpec::~ObLimitVecSpec() {}

OB_SERIALIZE_MEMBER((ObLimitVecSpec, ObOpSpec),
                    limit_expr_,
                    offset_expr_,
                    percent_expr_,
                    calc_found_rows_,
                    is_top_limit_,
                    is_fetch_with_ties_,
                    sort_columns_);

ObLimitVecOp::ObLimitVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      limit_(-1),
      offset_(0),
      input_cnt_(0),
      output_cnt_(0),
      total_cnt_(0),
      is_percent_first_(false),
      pre_sort_columns_(exec_ctx.get_allocator())
{
}

ObLimitVecOp::~ObLimitVecOp() { destroy(); }

int ObLimitVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  bool is_null_value = false;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit vector operator has no child", K(ret));
  } else if (OB_FAIL(ObLimitVecOp::get_int_val(MY_SPEC.limit_expr_, eval_ctx_, limit_, is_null_value))) {
    LOG_WARN("get limit values failed", K(ret));
  } else if (!is_null_value && OB_FAIL(ObLimitVecOp::get_int_val(MY_SPEC.offset_expr_, eval_ctx_,
                                                   offset_, is_null_value))) {
    LOG_WARN("get offset values failed", K(ret));
  } else if (is_null_value) {
    offset_ = 0;
    limit_ = 0;
  } else {
    is_percent_first_ = NULL != MY_SPEC.percent_expr_;
    // revise limit, offset because rownum < -1 is rewritten as limit -1
    // if *limit* < 0, then *offset_* is meaningless, so check both here together
    //offset 2 rows fetch next -3 rows only --> is meaningless
    offset_ = offset_ < 0 ? 0 : offset_;
    if (MY_SPEC.limit_expr_ != NULL) {//can not just set to 0, since there maybe only *offset* but not *limit* in a sql query
      limit_ = limit_ < 0 ? 0 : limit_;
    }
    pre_sort_columns_.reuse_ = true;
  }

  return ret;
}

int ObLimitVecOp::inner_rescan()
{
  input_cnt_ = 0;
  output_cnt_ = 0;
  return ObOperator::inner_rescan();
}

int ObLimitVecOp::inner_get_next_row()
{
  return OB_ERR_UNEXPECTED;
}


// Batch version of ObLimitOp::inner_get_next_row
int ObLimitVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  LOG_DEBUG("ObLimitVecOp[3] get_next_batch start", K(limit_), K(offset_), K(batch_cnt),
             K(is_percent_first_), K(output_cnt_), K(MY_SPEC.calc_found_rows_));
  const ObBatchRows *child_brs = nullptr;
  clear_evaluated_flag();
  while (OB_SUCC(ret) && input_cnt_ < offset_) {
    // Note: batch_cnt is NEVER bigger than offset
    if (input_cnt_ + batch_cnt > offset_) {
      batch_cnt = offset_ - input_cnt_;
    }
    if (OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
      LOG_WARN("child_op failed to get next row", K(input_cnt_), K(offset_), K(ret));
    } else if (is_percent_first_ && OB_FAIL(convert_limit_percent())) {
      LOG_WARN("failed to convert limit percent", K(ret));
    } else {
      input_cnt_ += (child_brs->size_ - child_brs->skip_->accumulate_bit_cnt(child_brs->size_));
    }
    if (OB_SUCC(ret) && child_brs->end_) {
      brs_.end_ = true;
      break;
    }
  } // end while

  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    bool skip_fetch_rows = false;
    if (input_cnt_ > offset_ && output_cnt_ == 0) {
      // offset error handling: child operator return more rows than expected
      // mark offset rows as skipped and continue limit logic
      brs_.copy(child_brs);
      input_cnt_ -= (brs_.size_ - brs_.skip_->accumulate_bit_cnt(brs_.size_));
      for (int64_t i = 0; i < child_brs->size_; i++) {
        if (brs_.skip_->at(i)) {
          continue;
        } else {
          ++input_cnt_;
          if (input_cnt_ <= offset_) {
            brs_.skip_->set(i);
          } else {
            break;
          }
        }
      }
      // stop getting rows from child and setting brs_ when skip_fetch_rows is true
      skip_fetch_rows = true;
    }

    // now the limit part
    int64_t left_count = 0;
    batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
    if (OB_UNLIKELY(brs_.end_) && !skip_fetch_rows) {
      brs_.size_ = 0;
      LOG_DEBUG("Offset num is bigger than child output num, return empty rows", K(offset_),
                K(input_cnt_), K(child_brs->size_), K(child_brs->end_));
    } else if (OB_SUCC(ret)) {
      if (is_percent_first_ || output_cnt_ < limit_ || limit_ < 0) {
        // adjust iterating count for last batch
        if (output_cnt_ + batch_cnt > limit_ && limit_ >= 0) {
          batch_cnt = limit_ - output_cnt_;
        }
        if (is_percent_first_) {
          // Fetch one row for percent first fetch, make sure %output_cnt_ never exceed %limit_
          // in this round.
          batch_cnt = 1;
        }

        if (!skip_fetch_rows && OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
          LOG_WARN("child_op failed to get next row", K(ret), K(limit_), K(batch_cnt));
        } else if (is_percent_first_ && OB_FAIL(convert_limit_percent())) {
          LOG_WARN("failed to convert limit percent", K(ret));
        } else if (limit_ == 0) {
          brs_.size_ = 0;
          brs_.end_ = true;
        } else {
          if (!skip_fetch_rows) {
            // skip copy brs_ from child as it is already copied in offset error
            // handing branch
            brs_.copy(child_brs);
          }
          output_cnt_ += (brs_.size_ - brs_.skip_->accumulate_bit_cnt(brs_.size_));
          if (output_cnt_ == limit_) {
            // Don't mark brs_.end_, end iterating in next round
            if (MY_SPEC.is_fetch_with_ties_) {
              ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
              batch_info_guard.set_batch_size(brs_.size_);
              batch_info_guard.set_batch_idx(
                find_last_available_row_cnt(*(brs_.skip_), brs_.size_));
              if (OB_FAIL(
                    pre_sort_columns_.save_store_row(MY_SPEC.sort_columns_, brs_, eval_ctx_))) {
                LOG_WARN("failed to deep copy limit last rows", K(ret));
              }
            }
          } else if (OB_UNLIKELY(limit_ != -1 /*limit=-1 means no limit in oracle mode*/
                                 && output_cnt_ > limit_)) {
            // Notice: here is the error hanlding branch
            // Child branch should NOT return rows more than batch_cnt.
            // If it return more row, something out of expect take place.
            // In order to keep limit logic NOT broken, update the batch size
            // within limit operator to get correct result.
            LOG_TRACE("child operator return more rows than expected", K(output_cnt_), K(limit_),
                      K(brs_), K(batch_cnt), K(child_->get_spec().get_type()));

            // recaculate output_cnt_ and update output brs_.size_
            output_cnt_ -= (brs_.size_ - brs_.skip_->accumulate_bit_cnt(brs_.size_));
            for (int64_t i = 0; i < child_brs->size_; i++) {
              if (brs_.skip_->at(i)) {
                continue;
              } else {
                ++output_cnt_;
                if (output_cnt_ >= limit_) {
                  brs_.size_ = i + 1;
                  break;
                }
              }
            }
          }
        }
        skip_fetch_rows = false;
      } else if (limit_ > 0 && output_cnt_ >= limit_ && MY_SPEC.is_fetch_with_ties_) {
        // keep fetching until a different value is found
        batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
        bool keep_iterating = false;
        uint32_t matched_row_count = 0;
        if (OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
          LOG_WARN("child_op failed to get next row", K(ret), K(limit_), K(batch_cnt),
                   K(child_brs->size_));
        } else if (OB_FAIL(compare_value_in_batch(keep_iterating, child_brs, matched_row_count))) {
          LOG_WARN("failed to is row order by item value equal", K(ret));
        }
        brs_.copy(child_brs);
        if (!keep_iterating) {
          brs_.end_ = true;
          brs_.size_ = matched_row_count;
        }
        output_cnt_ += brs_.size_;
      } else {
        brs_.end_ = true;
        if (MY_SPEC.calc_found_rows_) {
          batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
          while (OB_SUCC(child_->get_next_batch(batch_cnt, child_brs))) {
            left_count +=
              (child_brs->size_ - child_brs->skip_->accumulate_bit_cnt(child_brs->size_));
            if (child_brs->end_) {
              break;
            }
          }
          if (OB_SUCCESS != ret) {
            LOG_WARN("fail to get next row from child", K(ret));
          }
        }
      }
    }
    if (brs_.end_) {
      if (MY_SPEC.is_top_limit_) {
        total_cnt_ = left_count + output_cnt_ + input_cnt_;
        ObPhysicalPlanCtx *plan_ctx = NULL;
        if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("get physical plan context failed");
        } else {
          NG_TRACE_EXT(found_rows, OB_ID(total_count), total_cnt_, OB_ID(input_count), input_cnt_);
          plan_ctx->set_found_rows(total_cnt_);
        }
      }
    }
    LOG_DEBUG("limit_vec_op get_next_batch finished", K(batch_cnt), K(output_cnt_), K(brs_),
              K(limit_), K(input_cnt_));
  }

  return ret;
}

// batch version for is_row_order_by_item_value_equal
int ObLimitVecOp::compare_value_in_batch(bool &keep_iterating,
                                      const ObBatchRows * brs,
                                      uint32_t &row_count_matched)
{
  int ret = OB_SUCCESS;
  keep_iterating = true;
  if (MY_SPEC.sort_columns_.empty()) {
    // %sort_columns_ is empty if order by const value, set is_equal to true directly.
    // pre_sort_columns_.compact_row_ is NULL here.
  } else {
    CK(compare_vectors_.empty());

    const int64_t size = MY_SPEC.sort_columns_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.sort_columns_.count(); ++i) {
      ObExpr *expr = MY_SPEC.sort_columns_.at(i);
      if (OB_FAIL(expr->eval_vector(eval_ctx_, *brs))) {
        LOG_WARN("expression evaluate failed", K(ret));
      } else if (OB_FAIL(compare_vectors_.push_back(expr->get_vector(eval_ctx_)))) {
        LOG_WARN("compare_vectors_ push back failed", K(ret));
      }
    }

    ObCompactRow* current_waiting_compare_row = pre_sort_columns_.compact_row_;
    for (uint32_t row_idx = 0; OB_SUCC(ret) && keep_iterating && row_idx < brs->size_; row_idx++) {
      if (brs->skip_->at(row_idx)) {
        continue;
      }
      for (int64_t col_idx = 0; OB_SUCC(ret) && keep_iterating && col_idx < compare_vectors_.count();
                   ++col_idx) {
        ObExpr *expr = MY_SPEC.sort_columns_.at(col_idx);
        int cmp_ret = 0;
        if (OB_FAIL(compare_vectors_[col_idx]->null_first_cmp(*expr, row_idx,
                                                  current_waiting_compare_row->is_null(col_idx),
                                                  current_waiting_compare_row->get_cell_payload(pre_sort_columns_.row_meta_, col_idx),
                                                  current_waiting_compare_row->get_length(pre_sort_columns_.row_meta_, col_idx),
                                                  cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else {
          keep_iterating = (0 == cmp_ret);
        }
      }
      if (keep_iterating) {
        row_count_matched++;
      }
    }
    compare_vectors_.reuse();
  }
  return ret;
}

//For *percent*, you need to convert it to the corresponding *limit* count based on the total number of rows
int ObLimitVecOp::convert_limit_percent()
{
  int ret = OB_SUCCESS;
  double percent = 0.0;
  if (OB_FAIL(ObLimitVecOp::get_double_val(MY_SPEC.percent_expr_, eval_ctx_, percent))) {
    LOG_WARN("failed to get double value", K(ret));
  } else if (percent > 0) {
    int64_t tot_count = 0;
    if (OB_UNLIKELY(limit_ != -1) || OB_ISNULL(child_) ||
        OB_UNLIKELY(child_->get_spec().get_type() != PHY_VEC_MATERIAL &&
                    child_->get_spec().get_type() != PHY_SORT &&
                    child_->get_spec().get_type() != PHY_VEC_SORT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(limit_), K(child_));
    } else if (child_->get_spec().get_type() == PHY_MATERIAL &&
               OB_FAIL(static_cast<ObMaterialOp *>(child_)->get_material_row_count(tot_count))) {
      LOG_WARN("failed to get op row count", K(ret));
    } else if (child_->get_spec().get_type() == PHY_VEC_MATERIAL &&
               OB_FAIL(static_cast<ObMaterialVecOp *>(child_)->get_material_row_count(tot_count))) {
      LOG_WARN("failed to get op row count", K(ret));
      //TODO: so far vec_sort havn't implement, so here will cast it to old sort op
      //It should be replaced as PHY_VEC_SORT after new vectorization sort op is implemented
    } else if (child_->get_spec().get_type() == PHY_SORT &&
               FALSE_IT(tot_count = static_cast<ObSortOp *>(child_)->get_sort_row_count())) {
      LOG_WARN("failed to get op row count", K(ret));
    } else if (child_->get_spec().get_type() == PHY_VEC_SORT &&
               FALSE_IT(tot_count = static_cast<ObSortVecOp *>(child_)->get_sort_row_count())) {
      LOG_WARN("failed to get op row count", K(ret));
    } else if (OB_UNLIKELY(tot_count < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid child op row count", K(tot_count), K(ret));
    } else if (percent < 100) {
      //Compatible with oracle, rounded up
      int64_t percent_int64 = static_cast<int64_t>(percent);
      int64_t offset = (tot_count * percent / 100 - tot_count * percent_int64 / 100) > 0 ? 1 : 0;
      limit_ = tot_count * percent_int64 / 100 +  offset;
      is_percent_first_ = false;
    } else {
      limit_ = tot_count;
      is_percent_first_ = false;
    }
  } else {
    limit_ = 0;
  }
  return ret;
}

int ObLimitVecOp::get_double_val(ObExpr *expr, ObEvalCtx &eval_ctx, double &val)
{
  int ret = OB_SUCCESS;
  if (NULL != expr) {
    OB_ASSERT(ob_is_double_tc(expr->datum_meta_.type_));
    int64_t skip_int = 0;
    ObBitVector *skip = to_bit_vector(&skip_int);
    if (OB_FAIL(expr->eval_vector(eval_ctx, *skip, 1, true))) {
      LOG_WARN("expr evaluate failed", K(ret), K(expr));
    } else if (expr->get_vector(eval_ctx)->is_null(0)) {
      val = 0.0;
    } else {
      val = expr->get_vector(eval_ctx)->get_double(0);
    }
  }
  return ret;
}

int ObLimitVecOp::get_int_val(ObExpr *expr, ObEvalCtx &eval_ctx, int64_t &val, bool &is_null_value)
{
  int ret = OB_SUCCESS;
  if (NULL != expr) {
    OB_ASSERT(ob_is_int_tc(expr->datum_meta_.type_));
    int64_t skip_int = 0;
    ObBitVector *skip = to_bit_vector(&skip_int);
    if (OB_FAIL(expr->eval_vector(eval_ctx, *skip, 1, true))) {
      LOG_WARN("expr evaluate failed", K(ret), K(expr));
    } else if (expr->get_vector(eval_ctx)->is_null(0)) {
      is_null_value = true;
      val = 0.0;
    } else {
      val = expr->get_vector(eval_ctx)->get_int(0);
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
