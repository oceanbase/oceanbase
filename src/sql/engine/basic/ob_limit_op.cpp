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
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_material_op.h"
#include "sql/engine/sort/ob_sort_op.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObLimitSpec::ObLimitSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      limit_expr_(NULL),
      offset_expr_(NULL),
      percent_expr_(NULL),
      calc_found_rows_(false),
      is_top_limit_(false),
      is_fetch_with_ties_(false),
      sort_columns_(alloc)
{}

OB_SERIALIZE_MEMBER((ObLimitSpec, ObOpSpec), limit_expr_, offset_expr_, percent_expr_, calc_found_rows_, is_top_limit_,
    is_fetch_with_ties_, sort_columns_);

ObLimitOp::ObLimitOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(exec_ctx, spec, input),
      limit_(-1),
      offset_(0),
      input_cnt_(0),
      output_cnt_(0),
      total_cnt_(0),
      is_percent_first_(false),
      pre_sort_columns_(exec_ctx.get_allocator())
{}

int ObLimitOp::inner_open()
{
  int ret = OB_SUCCESS;
  bool is_null_value = false;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit operator has no child", K(ret));
  } else if (OB_FAIL(get_int_val(MY_SPEC.limit_expr_, eval_ctx_, limit_, is_null_value))) {
    LOG_WARN("get limit values failed", K(ret));
  } else if (!is_null_value && OB_FAIL(get_int_val(MY_SPEC.offset_expr_, eval_ctx_, offset_, is_null_value))) {
    LOG_WARN("get offset values failed", K(ret));
  } else if (is_null_value) {
    offset_ = 0;
    limit_ = 0;
  } else {
    is_percent_first_ = NULL != MY_SPEC.percent_expr_;
    // revise limit, offset because rownum < -1 is rewritten as limit -1
    // offset 2 rows fetch next -3 rows only --> is meaningless
    offset_ = offset_ < 0 ? 0 : offset_;
    if (MY_SPEC.limit_expr_ != NULL) {
      limit_ = limit_ < 0 ? 0 : limit_;
    }
    pre_sort_columns_.reuse_ = true;
  }

  return ret;
}

int ObLimitOp::rescan()
{
  input_cnt_ = 0;
  output_cnt_ = 0;
  return ObOperator::rescan();
}

int ObLimitOp::get_int_val(ObExpr* expr, ObEvalCtx& eval_ctx, int64_t& val, bool& is_null_value)
{
  int ret = OB_SUCCESS;
  if (NULL != expr) {
    OB_ASSERT(ob_is_int_tc(expr->datum_meta_.type_));
    ObDatum* datum = NULL;
    if (OB_FAIL(expr->eval(eval_ctx, datum))) {
      LOG_WARN("expr evaluate failed", K(ret), K(expr));
    } else if (datum->null_) {
      is_null_value = true;
      val = 0;
    } else {
      val = *datum->int_;
    }
  }
  return ret;
}

int ObLimitOp::get_double_val(ObExpr* expr, ObEvalCtx& eval_ctx, double& val)
{
  int ret = OB_SUCCESS;
  if (NULL != expr) {
    OB_ASSERT(ob_is_double_tc(expr->datum_meta_.type_));
    ObDatum* datum = NULL;
    if (OB_FAIL(expr->eval(eval_ctx, datum))) {
      LOG_WARN("expr evaluate failed", K(ret), K(expr));
    } else if (datum->null_) {
      val = 0.0;
    } else {
      val = *datum->double_;
    }
  }
  return ret;
}

// copy from ObLimit::inner_get_next_row
int ObLimitOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  while (OB_SUCC(ret) && input_cnt_ < offset_) {
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("child_op failed to get next row", K(input_cnt_), K(offset_), K(ret));
      }
    } else if (is_percent_first_ && OB_FAIL(convert_limit_percent())) {
      LOG_WARN("failed to convert limit percent", K(ret));
    } else {
      ++input_cnt_;
    }
  }  // end while

  /*
   * 1. is_percent_first_: for 'select * from t1 fetch next 50 percent rows only',
   *    need set lower block operators like sort or agg, and then reset is_percent_first_ to false.
   * 2. is_fetch_with_ties_: when we get enough rows as limit count, shall we keep fetching for
   *    those rows which equal to the last row of specified order (by order by clause) ?
   */
  int64_t left_count = 0;
  if (OB_SUCC(ret)) {
    if (is_percent_first_ || output_cnt_ < limit_ || limit_ < 0) {
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("child_op failed to get next row", K(ret), K_(limit), K_(offset), K_(input_cnt), K_(output_cnt));
        }
      } else if (is_percent_first_ && OB_FAIL(convert_limit_percent())) {
        LOG_WARN("failed to convert limit percent", K(ret));
      } else if (limit_ == 0) {
        ret = OB_ITER_END;
      } else {
        ++output_cnt_;
        LOG_DEBUG("output row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
        if (MY_SPEC.is_fetch_with_ties_ && output_cnt_ == limit_ &&
            OB_FAIL(pre_sort_columns_.save_store_row(MY_SPEC.sort_columns_, eval_ctx_))) {
          LOG_WARN("failed to deep copy limit last rows", K(ret));
        }
      }
    } else if (limit_ > 0 && MY_SPEC.is_fetch_with_ties_) {
      bool is_equal = false;
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("child_op failed to get next row", K(ret), K_(limit), K_(offset), K_(input_cnt), K_(output_cnt));
        }
      } else if (OB_FAIL(is_row_order_by_item_value_equal(is_equal))) {
        LOG_WARN("failed to is row order by item value equal", K(ret));
      } else if (is_equal) {
        ++output_cnt_;
      } else {
        ret = OB_ITER_END;
      }
    } else {
      ret = OB_ITER_END;
      if (MY_SPEC.calc_found_rows_) {
        while (OB_SUCC(child_->get_next_row())) {
          ++left_count;
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row from child", K(ret));
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    if (MY_SPEC.is_top_limit_) {
      total_cnt_ = left_count + output_cnt_ + input_cnt_;
      ObPhysicalPlanCtx* plan_ctx = NULL;
      if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("get physical plan context failed");
      } else {
        NG_TRACE_EXT(found_rows, OB_ID(total_count), total_cnt_, OB_ID(input_count), input_cnt_);
        plan_ctx->set_found_rows(total_cnt_);
      }
    }
  }
  return ret;
}

int ObLimitOp::is_row_order_by_item_value_equal(bool& is_equal)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.sort_columns_.empty()) {
    // %sort_columns_ is empty if order by const value, set is_equal to true directly.
    // pre_sort_columns_.store_row_ is NULL here.
    is_equal = true;
  } else {
    is_equal = true;
    CK(NULL != pre_sort_columns_.store_row_ && pre_sort_columns_.store_row_->cnt_ == MY_SPEC.sort_columns_.count());
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < MY_SPEC.sort_columns_.count(); ++i) {
      const ObExpr* expr = MY_SPEC.sort_columns_.at(i);
      ObDatum* datum = NULL;
      if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
        LOG_WARN("expression evaluate failed", K(ret));
      } else {
        is_equal = 0 == expr->basic_funcs_->null_first_cmp_(pre_sort_columns_.store_row_->cells()[i], *datum);
      }
    }
  }
  return ret;
}

int ObLimitOp::convert_limit_percent()
{
  int ret = OB_SUCCESS;
  double percent = 0.0;
  if (OB_FAIL(get_double_val(MY_SPEC.percent_expr_, eval_ctx_, percent))) {
    LOG_WARN("failed to get double value", K(ret));
  } else if (percent > 0) {
    int64_t tot_count = 0;
    if (OB_UNLIKELY(limit_ != -1) || OB_ISNULL(child_) ||
        OB_UNLIKELY(child_->get_spec().get_type() != PHY_MATERIAL && child_->get_spec().get_type() != PHY_SORT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(limit_), K(child_));
    } else if (child_->get_spec().get_type() == PHY_MATERIAL &&
               OB_FAIL(static_cast<ObMaterialOp*>(child_)->get_material_row_count(tot_count))) {
      LOG_WARN("failed to get op row count", K(ret));
    } else if (child_->get_spec().get_type() == PHY_SORT &&
               FALSE_IT(tot_count = static_cast<ObSortOp*>(child_)->get_sort_row_count())) {
      LOG_WARN("failed to get op row count", K(ret));
    } else if (OB_UNLIKELY(tot_count < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid child op row count", K(tot_count), K(ret));
    } else if (percent < 100) {
      int64_t percent_int64 = static_cast<int64_t>(percent);
      int64_t offset = (tot_count * percent / 100 - tot_count * percent_int64 / 100) > 0 ? 1 : 0;
      limit_ = tot_count * percent_int64 / 100 + offset;
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

}  // end namespace sql
}  // end namespace oceanbase
