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
#include "sql/engine/window_function/ob_window_function_op.h"
#include "lib/utility/utility.h"
#include "share/object/ob_obj_cast.h"
#include "common/row/ob_row_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_func_ceil.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/engine/expr/ob_expr_minus.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER(
    WinFuncInfo::ExtBound, is_preceding_, is_unbounded_, is_nmb_literal_, between_value_expr_, range_bound_expr_);

OB_SERIALIZE_MEMBER(WinFuncInfo, win_type_, func_type_, is_ignore_null_, is_from_first_, expr_, aggr_info_, upper_,
    lower_, param_exprs_, partition_exprs_, sort_exprs_, sort_collations_, sort_cmp_funcs_);

OB_SERIALIZE_MEMBER((ObWindowFunctionSpec, ObOpSpec), wf_infos_, all_expr_, is_parallel_);

DEF_TO_STRING(ObWindowFunctionSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(wf_infos));
  J_OBJ_END();
  return pos;
}

int ObWindowFunctionOp::AggrCell::trans_self(const ObRADatumStore::StoredRow& row)
{
  int ret = OB_SUCCESS;
  ObAggregateProcessor::GroupRow* group_row = NULL;
  if (!finish_prepared_ && OB_FAIL(aggr_processor_.init_one_group())) {
    LOG_WARN("fail to prepare the aggr func", K(ret), K(row));
  } else if (OB_FAIL(aggr_processor_.get_group_row(0, group_row))) {
    LOG_WARN("failed to get_group_row", K(ret));
  } else if (OB_ISNULL(group_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group_row is null", K(ret));
  } else if (!finish_prepared_) {
    if ((OB_FAIL(aggr_processor_.prepare(*group_row)))) {
      LOG_WARN("fail to prepare the aggr func", K(ret), K(row));
    } else {
      finish_prepared_ = true;
    }
  } else {
    if (OB_FAIL(aggr_processor_.process(*group_row))) {
      LOG_WARN("fail to process the aggr func", K(ret), K(row));
    }
  }

  if (OB_SUCC(ret)) {
    // uppon invoke trans(), forbiden it to reuse the last_result
    got_result_ = false;
  }
  return ret;
}

int ObWindowFunctionOp::AggrCell::final(ObDatum& val)
{
  int ret = OB_SUCCESS;
  if (!got_result_) {
    if (OB_FAIL(aggr_processor_.collect(0))) {
      LOG_WARN("fail to collect", K(ret));
    } else {
      val = wf_info_.aggr_info_.expr_->locate_expr_datum(op_.eval_ctx_);
      if (OB_FAIL(aggr_processor_.clone_cell(result_, val, wf_info_.aggr_info_.expr_->obj_meta_.is_number()))) {
        LOG_WARN("fail to clone_cell", K(ret));
      } else {
        got_result_ = true;
      }
    }
  } else {
    val = static_cast<ObDatum>(result_);
  }
  return ret;
}

DEF_TO_STRING(ObWindowFunctionOp::AggrCell)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("wf_cell");
  J_COLON();
  pos += ObWindowFunctionOp::WinFuncCell::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(finish_prepared), K_(result));
  J_OBJ_END();
  return pos;
}

int ObWindowFunctionOp::get_param_int_value(
    ObExpr& expr, ObEvalCtx& eval_ctx, bool& is_null, int64_t& value, const bool need_number /* = false*/)
{
  int ret = OB_SUCCESS;
  ObDatum* result = NULL;
  is_null = false;
  value = 0;
  if (OB_FAIL(expr.eval(eval_ctx, result))) {
    LOG_WARN("eval failed", K(ret));
  } else if (need_number && !expr.obj_meta_.is_number()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params is not number type", K(expr), K(ret));
  } else if (result->is_null()) {
    is_null = true;
  } else if (need_number || expr.obj_meta_.is_number()) {
    // we restrict the bucket_num in range [0, (1<<63)-1]
    number::ObNumber result_nmb;
    number::ObCompactNumber& cnum = const_cast<number::ObCompactNumber&>(result->get_number());
    result_nmb.assign(cnum.desc_.desc_, cnum.digits_ + 0);
    if (OB_FAIL(result_nmb.extract_valid_int64_with_trunc(value))) {
      LOG_WARN("extract_valid_int64_with_trunc failed", K(ret));
    }
  } else {
    switch (expr.obj_meta_.get_type_class()) {
      case ObIntTC: {
        value = result->get_int();
        break;
      }
      case ObUIntTC: {
        const uint64_t tmp_value = result->get_uint();
        if (tmp_value > INT64_MAX) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("int64 out of range", K(ret), K(tmp_value), K(INT64_MAX));
        } else {
          value = static_cast<int64_t>(tmp_value);
        }
        break;
      }
      case ObFloatTC: {
        const float tmp_value = result->get_float();
        constexpr float overflow_float_int64 = static_cast<float>(INT64_MAX);
        if (tmp_value >= overflow_float_int64) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("int64 out of range", K(ret), K(tmp_value), K(INT64_MAX));
        } else {
          value = static_cast<int64_t>(tmp_value);
        }
        break;
      }
      case ObDoubleTC: {
        const double tmp_value = result->get_double();
        constexpr double overflow_double_int64 = static_cast<double>(INT64_MAX);
        if (tmp_value >= overflow_double_int64) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("int64 out of range", K(ret), K(tmp_value), K(INT64_MAX));
        } else {
          value = static_cast<int64_t>(tmp_value);
        }
        break;
      }
      case ObBitTC: {
        const uint64_t tmp_value = result->get_bit();
        if (tmp_value > INT64_MAX) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("int64 out of range", K(ret), K(tmp_value), K(INT64_MAX));
        } else {
          value = static_cast<int64_t>(tmp_value);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not support type", K(expr), K(ret));
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::NonAggrCellRowNumber::eval(RowsReader& row_reader, const int64_t row_idx,
    const ObRADatumStore::StoredRow& row, const Frame& frame, ObDatum& val)
{
  int ret = OB_SUCCESS;
  UNUSED(row_reader);
  UNUSED(row_idx);
  UNUSED(row);
  UNUSED(frame);
  ObDatum& expr_datum = wf_info_.expr_->locate_datum_for_write(op_.eval_ctx_);
  int64_t row_number = row_idx - frame.head_ + 1;
  wf_info_.expr_->get_eval_info(op_.eval_ctx_).evaluated_ = true;
  if (share::is_oracle_mode()) {
    number::ObNumber res_nmb;
    ObNumStackAllocator<3> tmp_alloc;
    if (OB_FAIL(res_nmb.from(row_number, tmp_alloc))) {
      LOG_WARN("failed to build number from int64_t", K(ret));
    } else {
      expr_datum.set_number(res_nmb);
    }
  } else {
    expr_datum.set_int(row_number);
  }
  val = expr_datum;
  return ret;
}

int ObWindowFunctionOp::NonAggrCellNthValue::eval(RowsReader& row_reader, const int64_t row_idx,
    const ObRADatumStore::StoredRow& row, const Frame& frame, ObDatum& val)
{
  UNUSED(row_idx);
  UNUSED(row);
  int ret = OB_SUCCESS;
  const ObExprPtrIArray& params = wf_info_.param_exprs_;
  int64_t nth_val = 0;
  bool is_null = false;
  if (OB_UNLIKELY(params.count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid number of params", K(params.count()), K(ret));
  } else if (OB_FAIL(ObWindowFunctionOp::get_param_int_value(*params.at(1), op_.eval_ctx_, is_null, nth_val))) {
    LOG_WARN("get_param_int_value failed", K(ret));
  } else if (OB_UNLIKELY(is_null || nth_val <= 0)) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("invalid argument", K(ret), K(is_null), K(nth_val));
  } else {
    const bool is_ignore_null = wf_info_.is_ignore_null_;
    const bool is_from_first = wf_info_.is_from_first_;

    int64_t k = 0;
    bool is_calc_nth = false;
    ObDatum* tmp_result = NULL;
    for (int64_t i = is_from_first ? frame.head_ : frame.tail_;
         OB_SUCC(ret) && (is_from_first ? (i <= frame.tail_) : (i >= frame.head_));
         is_from_first ? ++i : --i) {
      const ObRADatumStore::StoredRow* a_row = NULL;
      tmp_result = NULL;
      if (OB_FAIL(row_reader.get_row(i, a_row))) {
        LOG_WARN("failed to get row", K(ret), K(i));
      } else if (FALSE_IT(op_.clear_evaluated_flag())) {
      } else if (OB_FAIL(a_row->to_expr(op_.get_all_expr(), op_.eval_ctx_))) {
        LOG_WARN("Failed to to_expr", K(ret));
      } else if (OB_FAIL(params.at(0)->eval(op_.eval_ctx_, tmp_result))) {
        LOG_WARN("fail to calc result row", K(ret));
      } else if ((!tmp_result->is_null() || !is_ignore_null) && ++k == nth_val) {
        is_calc_nth = true;
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (tmp_result != NULL) {
        val = *tmp_result;
      }
      if (!is_calc_nth) {
        val.set_null();
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::NonAggrCellLeadOrLag::eval(RowsReader& row_reader, const int64_t row_idx,
    const ObRADatumStore::StoredRow& row, const Frame& frame, ObDatum& val)
{
  int ret = OB_SUCCESS;
  UNUSED(row_reader);
  UNUSED(row_idx);
  UNUSED(row);
  UNUSED(frame);
  UNUSED(val);
  const ObExprPtrIArray& params = wf_info_.param_exprs_;
  // LEAD provides access to a row at a given physical offset beyond that position
  // while LAG provides access to a row at a given physical offset prior to that position.
  const bool is_lead = T_WIN_FUN_LEAD == wf_info_.func_type_;
  int lead_lag_offset_direction = is_lead ? +1 : -1;
  // 0 -> value_expr 1 -> offset 2 -> default value
  ObDatum lead_lag_params[3];
  enum LeadLagParamType { VALUE_EXPR = 0, OFFSET = 1, DEFAULT_VALUE = 2, NUM_LEAD_LAG_PARAMS };
  // if not specified, the default offset is 1.
  bool is_lead_lag_offset_used = false;
  //  lead_lag_params[OFFSET].set_int(1);

  // if not specified, the default value is NULL.
  lead_lag_params[DEFAULT_VALUE].set_null();

  if (OB_UNLIKELY(params.count() > NUM_LEAD_LAG_PARAMS || params.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid number of params", K(ret));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < params.count(); ++j) {
      ObDatum* result = NULL;
      if (OB_ISNULL(params.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param", K(ret));
      } else if (OB_FAIL(params.at(j)->eval(op_.eval_ctx_, result))) {
        LOG_WARN("fail to calc result row", K(ret));
      } else {
        lead_lag_params[j] = *result;
        is_lead_lag_offset_used |= (j == OFFSET);
      }
    }
    int64_t offset = 0;
    if (OB_SUCC(ret)) {
      if (is_lead_lag_offset_used) {
        bool is_null = false;
        if (OB_FAIL(ObWindowFunctionOp::get_param_int_value(*params.at(OFFSET), op_.eval_ctx_, is_null, offset))) {
          LOG_WARN("get_param_int_value failed", K(ret));
        } else if (OB_UNLIKELY(
                       is_null || offset < 0 || (share::is_oracle_mode() && wf_info_.is_ignore_null_ && offset == 0))) {
          ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
          if (!is_null) {
            LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, offset);
          }
          LOG_WARN("lead/lag argument is out of range", K(ret), K(is_null), K(offset));
        }
      } else {
        offset = 1;
      }
    }

    if (OB_SUCC(ret)) {
      int64_t step = 0;
      bool found = false;
      for (int64_t j = row_idx; OB_SUCC(ret) && !found && j >= frame.head_ && j <= frame.tail_;
           j += lead_lag_offset_direction) {
        const ObRADatumStore::StoredRow* a_row = NULL;
        ObDatum* tmp_result = NULL;
        if (OB_FAIL(row_reader.get_row(j, a_row))) {
          LOG_WARN("failed to get row", K(ret), K(j));
        } else if (FALSE_IT(op_.clear_evaluated_flag())) {
        } else if (OB_FAIL(a_row->to_expr(op_.get_all_expr(), op_.eval_ctx_))) {
          LOG_WARN("Failed to to_expr", K(ret));
        } else if (OB_FAIL(params.at(0)->eval(op_.eval_ctx_, tmp_result))) {
          LOG_WARN("fail to calc result row", K(ret));
        } else {
          lead_lag_params[VALUE_EXPR] = *tmp_result;
          if (wf_info_.is_ignore_null_ && tmp_result->is_null()) {
            step = (j == row_idx) ? step + 1 : step;
          } else if (step++ == offset) {
            found = true;
            val = *tmp_result;
          }
        }
      }
      if (OB_SUCC(ret) && !found) {
        val = lead_lag_params[DEFAULT_VALUE];
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::NonAggrCellNtile::eval(RowsReader& row_reader, const int64_t row_idx,
    const ObRADatumStore::StoredRow& row, const Frame& frame, ObDatum& val)
{
  int ret = OB_SUCCESS;
  UNUSED(row_reader);
  UNUSED(row);
  UNUSED(row_reader);
  UNUSED(row_idx);
  UNUSED(row);
  UNUSED(frame);
  UNUSED(val);
  /**
   * let total represent total rows in partition
   * let part_row_idx represent row_idx in partition (0, 1, 2 ...)
   * let x = total / bucket_num, y = total % bucket_num
   * so total = xb + y = xb + xy - xy + y = (x+1)y + x(b-y)
   * it means there are y buckets which contain (x+1) elements
   * there are (b-y) buckets which contain x elements
   *    total 5 elements divide into 3 bucket
   *    5/3=1..2, 1st bucket contains two elements and 2nd and 3rd bucket contain one element
   *    ------------------------
   *    | 1,1   | 2,2   | 3    |
   *    ------------------------
   * if (x == 0) { //not each bucket has one element
   *   result = part_row_idx + 1
   * } else {
   *   if (part_row_idx < y * (x + 1))
   *     result = part_row_idx / (x + 1) + 1
   *   else
   *     result = (part_row_idx - (y * (x + 1))) / x + y + 1
   * }
   */
  ObExpr* param = NULL;
  int64_t bucket_num = 0;
  const ObExprPtrIArray& params = wf_info_.param_exprs_;
  bool is_null = false;
  if (OB_UNLIKELY(params.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The number of arguments of NTILE should be 1", K(params.count()), K(ret));
  } else if (OB_ISNULL(param = params.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(ret));
  } else if (OB_FAIL(ObWindowFunctionOp::get_param_int_value(*param, op_.eval_ctx_, is_null, bucket_num))) {
    LOG_WARN("get_param_int_value failed", K(ret));
  } else if (is_null) {
    // return NULL when backets_num is NULL
    val.set_null();
  } else if (!is_oracle_mode() && !param->obj_meta_.is_numeric_type()) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("invalid argument", K(ret), K(param->obj_meta_));
  } else if (OB_UNLIKELY(bucket_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bucket_num is invalid", K(ret), K(bucket_num));
  } else {
    const int64_t total = frame.tail_ - frame.head_ + 1;
    const int64_t x = total / bucket_num;
    const int64_t y = total % bucket_num;
    const int64_t f_row_idx = row_idx - frame.head_;
    int64_t result = 0;
    LOG_DEBUG("print ntile param", K(total), K(x), K(y), K(f_row_idx));
    if (0 == x) {
      result = f_row_idx + 1;
    } else {
      if (f_row_idx < (y * (x + 1))) {
        result = f_row_idx / (x + 1) + 1;
      } else {
        result = (f_row_idx - (y * (x + 1))) / x + y + 1;
      }
    }
    ObDatum& expr_datum = wf_info_.expr_->locate_datum_for_write(op_.eval_ctx_);
    expr_datum.set_int(result);
    wf_info_.expr_->get_eval_info(op_.eval_ctx_).evaluated_ = true;
    val = expr_datum;
    LOG_DEBUG("ntile print result", K(result), K(bucket_num));
  }
  return ret;
}

int ObWindowFunctionOp::NonAggrCellRankLike::eval(RowsReader& row_reader, const int64_t row_idx,
    const ObRADatumStore::StoredRow& row, const Frame& frame, ObDatum& val)
{
  int ret = OB_SUCCESS;
  UNUSED(row_reader);
  UNUSED(row_idx);
  UNUSED(row);
  UNUSED(frame);
  UNUSED(val);
  bool equal_with_prev_row = false;
  if (row_idx != frame.head_) {
    equal_with_prev_row = true;
    ExprFixedArray& sort_cols = wf_info_.sort_exprs_;
    ObSortCollations& sort_collations = wf_info_.sort_collations_;
    ObSortFuncs& sort_cmp_funcs = wf_info_.sort_cmp_funcs_;
    const ObRADatumStore::StoredRow* tmp_row = NULL;
    if (OB_FAIL(row_reader.get_row(row_idx - 1, tmp_row))) {
      LOG_WARN("failed to get row", K(ret), K(row_idx));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < sort_cols.count(); ++i) {
        ObDatumCmpFuncType cmp_func = sort_cmp_funcs.at(i).cmp_func_;
        const int64_t idx = sort_collations.at(i).field_idx_;
        const ObDatum& l_datum = tmp_row->cells()[idx];
        const ObDatum& r_datum = row.cells()[idx];
        int match = cmp_func(l_datum, r_datum);
        LOG_DEBUG("cmp ", K(idx), K(l_datum), K(r_datum), K(match));
        if (0 != match) {
          equal_with_prev_row = false;
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t rank = -1;
    if (equal_with_prev_row) {
      rank = rank_of_prev_row_;
    } else if (T_WIN_FUN_RANK == wf_info_.func_type_ || T_WIN_FUN_PERCENT_RANK == wf_info_.func_type_) {
      rank = row_idx - frame.head_ + 1;
    } else if (T_WIN_FUN_DENSE_RANK == wf_info_.func_type_) {
      // dense_rank
      rank = rank_of_prev_row_ + 1;
    }
    ObDatum& expr_datum = wf_info_.expr_->locate_datum_for_write(op_.eval_ctx_);
    wf_info_.expr_->get_eval_info(op_.eval_ctx_).evaluated_ = true;
    if (T_WIN_FUN_PERCENT_RANK == wf_info_.func_type_) {
      // result will be zero when only one row within frame
      if (0 == frame.tail_ - frame.head_) {
        number::ObNumber res_nmb;
        res_nmb.set_zero();
        expr_datum.set_number(res_nmb);
      } else {
        number::ObNumber numerator;
        number::ObNumber denominator;
        number::ObNumber res_nmb;
        ObNumStackAllocator<3> tmp_alloc;
        if (OB_FAIL(numerator.from(rank - 1, tmp_alloc))) {
          LOG_WARN("failed to build number from int64_t", K(ret));
        } else if (OB_FAIL(denominator.from(frame.tail_ - frame.head_, tmp_alloc))) {
          LOG_WARN("failed to build number from int64_t", K(ret));
        } else if (OB_FAIL(numerator.div(denominator, res_nmb, tmp_alloc))) {
          LOG_WARN("failed to div number", K(ret));
        } else {
          expr_datum.set_number(res_nmb);
        }
      }
    } else if (share::is_oracle_mode()) {
      number::ObNumber res_nmb;
      ObNumStackAllocator<3> tmp_alloc;
      if (OB_FAIL(res_nmb.from(rank, tmp_alloc))) {
        LOG_WARN("failed to build number from int64_t", K(ret));
      } else {
        expr_datum.set_number(res_nmb);
      }
    } else {
      expr_datum.set_int(rank);
    }

    if (OB_SUCC(ret)) {
      rank_of_prev_row_ = rank;
      val = static_cast<ObDatum&>(expr_datum);
    }
  }
  return ret;
}

DEF_TO_STRING(ObWindowFunctionOp::NonAggrCellRankLike)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("wf_cell");
  J_COLON();
  pos += ObWindowFunctionOp::WinFuncCell::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(rank_of_prev_row));
  J_OBJ_END();
  return pos;
}

int ObWindowFunctionOp::NonAggrCellCumeDist::eval(RowsReader& row_reader, const int64_t row_idx,
    const ObRADatumStore::StoredRow& row, const Frame& frame, ObDatum& val)
{
  int ret = OB_SUCCESS;
  UNUSED(row_reader);
  UNUSED(row_idx);
  UNUSED(row);
  UNUSED(frame);
  UNUSED(val);
  int64_t same_idx = row_idx;
  bool should_continue = true;
  const ObRADatumStore::StoredRow& ref_row = row;
  ExprFixedArray& sort_cols = wf_info_.sort_exprs_;
  ObSortCollations& sort_collations = wf_info_.sort_collations_;
  ObSortFuncs& sort_cmp_funcs = wf_info_.sort_cmp_funcs_;
  while (should_continue && OB_SUCC(ret) && same_idx < frame.tail_) {
    const ObRADatumStore::StoredRow* a_row = NULL;
    if (OB_FAIL(row_reader.get_row(same_idx + 1, a_row))) {
      LOG_WARN("fail to get row", K(ret), K(same_idx));
    } else {
      const ObRADatumStore::StoredRow& iter_row = *a_row;
      for (int64_t i = 0; should_continue && OB_SUCC(ret) && i < sort_cols.count(); i++) {
        ObDatumCmpFuncType cmp_func = sort_cmp_funcs.at(i).cmp_func_;
        const int64_t idx = sort_collations.at(i).field_idx_;
        const ObDatum& l_datum = ref_row.cells()[idx];
        const ObDatum& r_datum = iter_row.cells()[idx];
        int match = cmp_func(l_datum, r_datum);
        LOG_DEBUG("cmp ", K(idx), K(l_datum), K(r_datum), K(match));
        if (0 != match) {
          should_continue = false;
        }
      }
      if (OB_SUCC(ret) && should_continue) {
        ++same_idx;
      }
    }
  }
  if (OB_SUCC(ret)) {
    // number of row[cur] >= row[:] (whether `>=` or other is depend on ORDER BY)
    number::ObNumber numerator;
    // total tuple of current window
    number::ObNumber denominator;
    // result number
    number::ObNumber res_nmb;
    ObNumStackAllocator<3> tmp_alloc;
    if (OB_FAIL(numerator.from(same_idx - frame.head_ + 1, tmp_alloc))) {
      LOG_WARN("failed to build number from int64_t", K(ret));
    } else if (OB_FAIL(denominator.from(frame.tail_ - frame.head_ + 1, tmp_alloc))) {
      LOG_WARN("failed to build number from int64_t", K(ret));
    } else if (OB_FAIL(numerator.div(denominator, res_nmb, tmp_alloc))) {
      LOG_WARN("failed to div number", K(ret));
    } else {
      ObDatum& expr_datum = wf_info_.expr_->locate_datum_for_write(op_.eval_ctx_);
      wf_info_.expr_->get_eval_info(op_.eval_ctx_).evaluated_ = true;
      expr_datum.set_number(res_nmb);
      val = static_cast<ObDatum&>(expr_datum);
    }
  }

  return ret;
}

int ObWindowFunctionOp::check_same_partition(WinFuncCell& cell, bool& same)
{
  int ret = OB_SUCCESS;
  same = true;
  const auto& exprs = cell.wf_info_.partition_exprs_;
  if (!exprs.empty()) {
    if (NULL == cell.part_values_.store_row_ || cell.part_values_.store_row_->cnt_ != exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current partition value not saved or cell count mismatch", K(ret), K(cell.part_values_));
    } else {
      ObDatum* val = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && same && i < exprs.count(); i++) {
        if (OB_FAIL(exprs.at(i)->eval(eval_ctx_, val))) {
          LOG_WARN("expression evaluate failed", K(ret));
        } else if (0 != exprs.at(i)->basic_funcs_->null_first_cmp_(*val, cell.part_values_.store_row_->cells()[i])) {
          same = false;
        }
      }
    }
  }
  return ret;
}

// todo: concern boundary
bool ObWindowFunctionOp::Frame::need_restart_aggr(
    const bool can_inv, const Frame& last_valid_frame, const Frame& new_frame)
{
  bool need = false;
  if (-1 == last_valid_frame.head_ || -1 == last_valid_frame.tail_) {
    need = true;
  } else {
    const int64_t inc_cost =
        std::abs(last_valid_frame.head_ - new_frame.head_) + std::abs(last_valid_frame.tail_ - new_frame.tail_);
    const int64_t restart_cost = new_frame.tail_ - new_frame.head_;
    if (inc_cost > restart_cost) {
      need = true;
    } else if (!can_inv) {
      // has sliding-out row
      if (new_frame.head_ > last_valid_frame.head_ || new_frame.tail_ < last_valid_frame.tail_) {
        need = true;
      }
    }
  }
  return need;
}

bool ObWindowFunctionOp::Frame::valid_frame(const Frame& part_frame, const Frame& frame)
{
  return frame.head_ <= frame.tail_ && frame.head_ <= part_frame.tail_ && frame.tail_ >= part_frame.head_;
}

bool ObWindowFunctionOp::Frame::same_frame(const Frame& left, const Frame& right)
{
  return left.head_ == right.head_ && left.tail_ == right.tail_;
}

void ObWindowFunctionOp::Frame::prune_frame(const Frame& part_frame, Frame& frame)
{
  // it's caller's responsibility for invoking valid_frame() first
  if (frame.head_ < part_frame.head_) {
    frame.head_ = part_frame.head_;
  }
  if (frame.tail_ > part_frame.tail_) {
    frame.tail_ = part_frame.tail_;
  }
}

template <class FuncType>
int ObWindowFunctionOp::FuncAllocer::alloc(
    WinFuncCell*& return_func, WinFuncInfo& wf_info, ObWindowFunctionOp& op, const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void* tmp_ptr = local_allocator_->alloc(sizeof(FuncType));
  if (OB_ISNULL(tmp_ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    return_func = new (tmp_ptr) FuncType(wf_info, op);
    ret = return_func->part_rows_store_.reset(tenant_id);
  }
  return ret;
}

int ObWindowFunctionOp::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!wf_list_.is_empty())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("wf_list_ is inited", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else {
    const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    local_allocator_.set_tenant_id(tenant_id);
    local_allocator_.set_label(ObModIds::OB_SQL_WINDOW_LOCAL);
    local_allocator_.set_ctx_id(ObCtxIds::WORK_AREA);
    FuncAllocer func_alloc;
    func_alloc.local_allocator_ = &local_allocator_;

    WFInfoFixedArray& wf_infos = *const_cast<WFInfoFixedArray*>(&MY_SPEC.wf_infos_);
    ret = curr_row_collect_values_.prepare_allocate(wf_infos.count());
    for (int64_t i = 0; i < wf_infos.count() && OB_SUCC(ret); ++i) {
      WinFuncInfo& wf_info = wf_infos.at(i);
      WinFuncCell* wf_cell = NULL;

      if (OB_SUCC(ret)) {
        switch (wf_info.func_type_) {
          case T_FUN_SUM:
          case T_FUN_MAX:
          case T_FUN_MIN:
          case T_FUN_COUNT:
          case T_FUN_AVG:
          case T_FUN_MEDIAN:
          case T_FUN_GROUP_PERCENTILE_CONT:
          case T_FUN_GROUP_PERCENTILE_DISC:
          case T_FUN_STDDEV:
          case T_FUN_STDDEV_SAMP:
          case T_FUN_VARIANCE:
          case T_FUN_STDDEV_POP:
          case T_FUN_APPROX_COUNT_DISTINCT:
          case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
          case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE:
          case T_FUN_GROUP_CONCAT:
          case T_FUN_CORR:
          case T_FUN_COVAR_POP:
          case T_FUN_COVAR_SAMP:
          case T_FUN_VAR_POP:
          case T_FUN_VAR_SAMP:
          case T_FUN_REGR_SLOPE:
          case T_FUN_REGR_INTERCEPT:
          case T_FUN_REGR_COUNT:
          case T_FUN_REGR_R2:
          case T_FUN_REGR_AVGX:
          case T_FUN_REGR_AVGY:
          case T_FUN_REGR_SXX:
          case T_FUN_REGR_SYY:
          case T_FUN_REGR_SXY:
          case T_FUN_KEEP_MAX:
          case T_FUN_KEEP_MIN:
          case T_FUN_KEEP_SUM:
          case T_FUN_KEEP_COUNT:
          case T_FUN_KEEP_WM_CONCAT:
          case T_FUN_WM_CONCAT: 
          case T_FUN_JSON_ARRAYAGG:
          case T_FUN_JSON_OBJECTAGG: {
            void* tmp_ptr = local_allocator_.alloc(sizeof(AggrCell));
            void* tmp_array = local_allocator_.alloc(sizeof(AggrInfoFixedArray));
            ObIArray<ObAggrInfo>* aggr_infos = NULL;
            if (OB_ISNULL(tmp_ptr) || OB_ISNULL(tmp_array)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to alloc", KP(tmp_ptr), KP(tmp_array), K(ret));
            } else if (FALSE_IT(aggr_infos = new (tmp_array) AggrInfoFixedArray(local_allocator_, 1))) {
            } else if (OB_FAIL(aggr_infos->push_back(wf_info.aggr_info_))) {
              LOG_WARN("failed to push_back", K(wf_info.aggr_info_), K(ret));
            } else {
              AggrCell* aggr_func = new (tmp_ptr) AggrCell(wf_info, *this, *aggr_infos);
              aggr_func->aggr_processor_.set_in_window_func();
              if (OB_FAIL(aggr_func->aggr_processor_.init())) {
                LOG_WARN("failed to initialize init_group_rows", K(ret));
              } else {
                wf_cell = aggr_func;
              }
            }
            break;
          }
          case T_WIN_FUN_RANK:
          case T_WIN_FUN_DENSE_RANK:
          case T_WIN_FUN_PERCENT_RANK: {
            ret = func_alloc.alloc<NonAggrCellRankLike>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          case T_WIN_FUN_CUME_DIST: {
            ret = func_alloc.alloc<NonAggrCellCumeDist>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          case T_WIN_FUN_ROW_NUMBER: {
            ret = func_alloc.alloc<NonAggrCellRowNumber>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          // first_value && last_value has been converted to nth_value when resolving
          // case T_WIN_FUN_FIRST_VALUE:
          // case T_WIN_FUN_LAST_VALUE:
          case T_WIN_FUN_NTH_VALUE: {
            ret = func_alloc.alloc<NonAggrCellNthValue>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          case T_WIN_FUN_LEAD:
          case T_WIN_FUN_LAG: {
            ret = func_alloc.alloc<NonAggrCellLeadOrLag>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          case T_WIN_FUN_NTILE: {
            ret = func_alloc.alloc<NonAggrCellNtile>(wf_cell, wf_info, *this, tenant_id);
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unsupported function", K(wf_info.func_type_), K(ret));
            break;
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(wf_cell)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("wf_cell is null", K(wf_info), K(ret));
        } else {
          wf_cell->wf_idx_ = i + 1;
          if (OB_UNLIKELY(!wf_list_.add_last(wf_cell))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("add func failed", K(ret));
          } else {
            LOG_DEBUG("add func succ", KPC(wf_cell));
          }
        }
      }
    }  // end of for
  }
  return ret;
}

int ObWindowFunctionOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_open())) {
    LOG_WARN("inner_open child operator failed", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (OB_FAIL(next_row_.init(local_allocator_, child_->get_spec().output_.count()))) {
    LOG_WARN("init shadow copy row failed", K(ret));
  } else if (OB_FAIL(init())) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(reset_for_scan(ctx_.get_my_session()->get_effective_tenant_id()))) {
    LOG_WARN("reset_for_scan failed", K(ret));
  }
  return ret;
}

int ObWindowFunctionOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::rescan())) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (OB_FAIL(reset_for_scan(ctx_.get_my_session()->get_effective_tenant_id()))) {
    LOG_WARN("reset_for_scan failed", K(ret));
  } else if (MY_SPEC.is_parallel_) {
    finish_parallel_ = false;
  }
  iter_end_ = false;
  return ret;
}

int ObWindowFunctionOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::switch_iterator())) {
    LOG_WARN("switch_iterator child operator failed", K(ret));
  } else if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (OB_FAIL(reset_for_scan(ctx_.get_my_session()->get_effective_tenant_id()))) {
    LOG_WARN("reset_for_scan failed", K(ret));
  }
  return ret;
}

int ObWindowFunctionOp::inner_close()
{
  curr_row_collect_values_.reset();
  rows_store_.rows_buf_.reset();
  DLIST_FOREACH_NORET(func, wf_list_)
  {
    if (func != NULL) {
      func->~WinFuncCell();
    }
  }
  wf_list_.reset();
  local_allocator_.reset();
  return ObOperator::inner_close();
}

void ObWindowFunctionOp::destroy()
{
  rows_store_.~RowsStore();
  wf_list_.~WinFuncCellList();
  local_allocator_.~ObArenaAllocator();
  ObOperator::destroy();
}

int ObWindowFunctionOp::fetch_child_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (next_row_valid_) {
    next_row_valid_ = false;
    ret = next_row_.restore(child_->get_spec().output_, eval_ctx_);
  } else {
    ret = child_->get_next_row();
    if (OB_ITER_END == ret) {
      child_iter_end_ = true;
    }
  }
  return ret;
}

int ObWindowFunctionOp::input_one_row(WinFuncCell& wf_cell, bool& part_end)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  bool is_same_part = false;
  if (OB_FAIL(fetch_child_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("child_op failed to get next row", K(ret));
    } else {
      part_end = true;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(check_same_partition(wf_cell, is_same_part))) {
    LOG_WARN("check same partition failed", K(ret));
  } else if (!is_same_part) {
    part_end = true;
    if (OB_FAIL(next_row_.shadow_copy(child_->get_spec().output_, eval_ctx_))) {
      LOG_WARN("shadow copy row failed", K(ret));
    } else {
      next_row_valid_ = true;
    }
  } else if (OB_FAIL(rows_store_.add_row(get_all_expr(), &eval_ctx_))) {
    LOG_WARN("add row failed", K(get_all_expr()), K(ret));
  }
  return ret;
}

int ObWindowFunctionOp::reset_for_part_scan(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ret = rows_store_.reset(tenant_id);
  last_output_row_idx_ = OB_INVALID_INDEX;

  DLIST_FOREACH(func, wf_list_)
  {
    ret = func->part_rows_store_.reset(tenant_id);
  }
  LOG_DEBUG("finish reset_for_part_scan", K(rows_store_), K(ret));
  return ret;
}

int ObWindowFunctionOp::compute(RowsReader& row_reader, WinFuncCell& wf_cell, const int64_t row_idx, ObDatum& val)
{
  int ret = OB_SUCCESS;
  const ObRADatumStore::StoredRow* row = NULL;

  Frame new_frame;
  bool upper_has_null = false;
  bool lower_has_null = false;
  if (OB_FAIL(rows_store_.get_row(row_idx, row))) {
    LOG_WARN("failed to get row", K(ret), K(row_idx));
  } else if (FALSE_IT(clear_evaluated_flag())) {
  } else if (OB_FAIL(row->to_expr(get_all_expr(), eval_ctx_))) {
    LOG_WARN("Failed to to_expr", K(ret));
  } else if (OB_FAIL(get_pos(row_reader, wf_cell, row_idx, *row, true, new_frame.head_, upper_has_null))) {
    LOG_WARN("get pos failed", K(ret));
  } else if (OB_FAIL(get_pos(row_reader, wf_cell, row_idx, *row, false, new_frame.tail_, lower_has_null))) {
    LOG_WARN("get pos failed", K(ret));
  } else {
    Frame& last_valid_frame = wf_cell.last_valid_frame_;
    Frame part_frame(wf_cell.part_first_row_idx_, get_part_end_idx());

    LOG_DEBUG("dump frame",
        K(part_frame),
        K(last_valid_frame),
        K(new_frame),
        K(rows_store_.count()),
        K(row_idx),
        K(upper_has_null),
        K(lower_has_null),
        K(wf_cell));
    if (!upper_has_null && !lower_has_null && Frame::valid_frame(part_frame, new_frame)) {
      Frame::prune_frame(part_frame, new_frame);
      if (wf_cell.is_aggr()) {
        AggrCell* aggr_func = static_cast<AggrCell*>(&wf_cell);
        const ObRADatumStore::StoredRow* cur_row = NULL;
        if (!Frame::same_frame(last_valid_frame, new_frame)) {
          if (!Frame::need_restart_aggr(aggr_func->can_inv(), last_valid_frame, new_frame)) {
            bool use_trans = new_frame.head_ < last_valid_frame.head_;
            int64_t b = min(new_frame.head_, last_valid_frame.head_);
            int64_t e = max(new_frame.head_, last_valid_frame.head_);
            for (int64_t i = b; OB_SUCC(ret) && i < e; ++i) {
              if (OB_FAIL(rows_store_.get_row(i, cur_row))) {
                LOG_WARN("get cur row failed", K(ret), K(i));
              } else if (FALSE_IT(clear_evaluated_flag())) {
              } else if (OB_FAIL(cur_row->to_expr(get_all_expr(), eval_ctx_))) {
                LOG_WARN("Failed to to_expr", K(ret));
              } else if (OB_FAIL(aggr_func->invoke_aggr(use_trans, *cur_row))) {
                LOG_WARN("invoke failed", K(use_trans), K(ret));
              }
            }
            use_trans = new_frame.tail_ > last_valid_frame.tail_;
            b = min(new_frame.tail_, last_valid_frame.tail_);
            e = max(new_frame.tail_, last_valid_frame.tail_);
            for (int64_t i = b + 1; OB_SUCC(ret) && i <= e; ++i) {
              if (OB_FAIL(rows_store_.get_row(i, cur_row))) {
                LOG_WARN("get cur row failed", K(ret), K(i));
              } else if (FALSE_IT(clear_evaluated_flag())) {
              } else if (OB_FAIL(cur_row->to_expr(get_all_expr(), eval_ctx_))) {
                LOG_WARN("Failed to to_expr", K(ret));
              } else if (OB_FAIL(aggr_func->invoke_aggr(use_trans, *cur_row))) {
                LOG_WARN("invoke failed", K(use_trans), K(ret));
              }
            }
          } else {
            aggr_func->reset_for_restart();
            LOG_DEBUG("restart agg", K(last_valid_frame), K(new_frame), KPC(aggr_func));
            for (int64_t i = new_frame.head_; OB_SUCC(ret) && i <= new_frame.tail_; ++i) {
              if (OB_FAIL(rows_store_.get_row(i, cur_row))) {
                LOG_WARN("get cur row failed", K(ret), K(i));
              } else if (FALSE_IT(clear_evaluated_flag())) {
              } else if (OB_FAIL(cur_row->to_expr(get_all_expr(), eval_ctx_))) {
                LOG_WARN("Failed to to_expr", K(ret));
              } else if (OB_FAIL(aggr_func->trans(*cur_row))) {
                LOG_WARN("trans failed", K(ret));
              }
            }
          }
        } else {
          LOG_DEBUG("use last value");
          // reuse last result, invoke final directly...
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(aggr_func->final(val))) {
            LOG_WARN("final failed", K(ret));
          } else {
            last_valid_frame = new_frame;
            LOG_DEBUG("finish compute", K(row_idx), K(last_valid_frame), K(val));
          }
        }
      } else {
        NonAggrCell* non_aggr_func = static_cast<NonAggrCell*>(&wf_cell);
        if (!Frame::same_frame(last_valid_frame, new_frame)) {
          non_aggr_func->reset_for_restart();
        }
        if (OB_FAIL(non_aggr_func->eval(row_reader, row_idx, *row, new_frame, val))) {
          LOG_WARN("eval failed", K(ret));
        } else {
          last_valid_frame = new_frame;
        }
      }
    } else {
      // special case
      if (T_FUN_COUNT == wf_cell.wf_info_.func_type_) {
        ObDatum& expr_datum = wf_cell.wf_info_.aggr_info_.expr_->locate_datum_for_write(eval_ctx_);
        expr_datum.set_int(0);
        wf_cell.wf_info_.aggr_info_.expr_->get_eval_info(eval_ctx_).evaluated_ = true;
        val = static_cast<ObDatum&>(expr_datum);
      } else {
        // set null for invalid frame
        val.set_null();
      }
    }
  }
  return ret;
}

int ObWindowFunctionOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("before inner_get_next_row",
      "total_size",
      this->local_allocator_.get_arena().total(),
      "used_size",
      this->local_allocator_.get_arena().used());
  if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ctx_.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  }

  const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  WinFuncCell* first = wf_list_.get_first();
  WinFuncCell* end = wf_list_.get_header();
  while (OB_SUCC(ret)) {
    if (child_iter_end_ && all_outputed()) {
      LOG_DEBUG("iter end", K(last_output_row_idx_));
      ret = OB_ITER_END;
    } else if (all_outputed()) {
      LOG_DEBUG("begin compute", K(rows_store_.count()), K(last_output_row_idx_));
      // load && compute
      if (OB_FAIL(reset_for_part_scan(tenant_id))) {
        LOG_WARN("fail to reset_for_part_scan", K(ret));
      }

      const static int64_t CHECK_STATUS_INTERVAL = 10000;
      int64_t check_times = 0;
      do {
        // <1> get first row
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(fetch_child_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get row from child failed", K(ret));
          } else if (MY_SPEC.is_parallel_ && !finish_parallel_) {
            // no row fetched, need broadcast this message too.
            if (OB_FAIL(parallel_winbuf_process())) {
              LOG_WARN("parallel window buf process failed");
            } else {
              ret = OB_ITER_END;
            }
          }

        } else if (OB_FAIL(rows_store_.add_row(get_all_expr(), &eval_ctx_))) {
          LOG_WARN("add row to row store failed", K(ret));
        }

        // <2> save partition by value
        for (WinFuncCell* wf = first; OB_SUCC(ret) && wf != end; wf = wf->get_next()) {
          wf->part_first_row_idx_ = wf->part_rows_store_.count();
          if (!wf->wf_info_.partition_exprs_.empty()) {
            if (OB_FAIL(wf->part_values_.save_store_row(wf->wf_info_.partition_exprs_, eval_ctx_))) {
              LOG_WARN("save current partition values failed", K(ret));
            }
          }
        }

        // <3> iterate child rows and check same partition with the first window function
        bool part_end = false;
        while (OB_SUCC(ret) && !part_end) {
          if (OB_FAIL(input_one_row(*first, part_end))) {
            LOG_WARN("input one row failed", K(ret));
          }
        }

        // <4> check the following window functions whether in same partition
        end = wf_list_.get_header();
        if (OB_FAIL(ret)) {
        } else if (child_iter_end_ || 1 == wf_list_.get_size()) {
          // all window functions end it current partition.
        } else {
          for (WinFuncCell* wf = first->get_next(); OB_SUCC(ret) && wf != wf_list_.get_header(); wf = wf->get_next()) {
            bool same = false;
            if (OB_FAIL(check_same_partition(*wf, same))) {
              LOG_WARN("check same partition failed", K(ret));
            } else if (same) {
              end = wf;
              break;
            }
          }
        }

        // <5> compute [first, end) window functions
        for (WinFuncCell* wf = first; OB_SUCC(ret) && wf != end; wf = wf->get_next()) {
          // reset func before compute
          wf->reset_for_restart();
          ObDatum result_datum;
          RowsReader row_reader(rows_store_);
          for (int64_t i = wf->part_first_row_idx_; i < rows_store_.count() && OB_SUCC(ret); ++i) {
            // we should check status interval since this loop will occupy cpu!
            if (0 == ++check_times % CHECK_STATUS_INTERVAL) {
              if (OB_FAIL(ctx_.check_status())) {
                break;
              }
            }
            if (OB_FAIL(compute(row_reader, *wf, i, result_datum))) {
              LOG_WARN("compute failed", K(ret));
            } else if (OB_FAIL(collect_result(i, result_datum, *wf))) {
              LOG_WARN("collect_result failed", K(ret));
            }
          }
          // free prev
          if (OB_SUCC(ret) && first != wf) {
            wf->get_prev()->part_rows_store_.reset_buf(tenant_id);
          }
        }
      } while (OB_SUCC(ret) && end != wf_list_.get_header());
    } else {
      if (MY_SPEC.is_parallel_ && !finish_parallel_ && OB_FAIL(parallel_winbuf_process())) {
        LOG_WARN("fail to parallel process window function buffer", K(ret));
      } else {
        WinFuncCell& wf_cell = *wf_list_.get_last();
        ++last_output_row_idx_;
        LOG_DEBUG("do output", "curr_output_row_idx", last_output_row_idx_, K(wf_cell), K(rows_store_.count()));
        // output
        const ObRADatumStore::StoredRow* child_row = NULL;
        const ObRADatumStore::StoredRow* result_row = NULL;
        if (OB_FAIL(rows_store_.get_row(last_output_row_idx_, child_row))) {
          LOG_WARN("get row failed", K(ret), K(last_output_row_idx_));
        } else if (OB_FAIL(child_row->to_expr(get_all_expr(), eval_ctx_))) {
          LOG_WARN("Failed to get next row", K(ret));
        } else if (MY_SPEC.is_parallel_ && OB_FAIL(wf_cell.part_rows_store_.get_row(0, result_row))) {
          LOG_WARN("get row failed", K(ret), K(last_output_row_idx_));
        } else if (!MY_SPEC.is_parallel_ &&
                   OB_FAIL(wf_cell.part_rows_store_.get_row(last_output_row_idx_, result_row))) {
          LOG_WARN("get row failed", K(ret), K(last_output_row_idx_));
        } else {
          clear_evaluated_flag();
          WinFuncCell* tmp_wf_cell = wf_list_.get_first();
          for (int64_t i = 0; i < result_row->cnt_ && tmp_wf_cell != NULL; ++i, tmp_wf_cell = tmp_wf_cell->get_next()) {
            tmp_wf_cell->wf_info_.expr_->locate_expr_datum(eval_ctx_) = result_row->cells()[i];
            tmp_wf_cell->wf_info_.expr_->get_eval_info(eval_ctx_).evaluated_ = true;
          }
          LOG_DEBUG(
              "finish output one row", "curr_output_row_idx", last_output_row_idx_, KPC(child_row), KPC(result_row));
          break;
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    iter_end_ = true;
    reset_for_scan(ctx_.get_my_session()->get_effective_tenant_id());
  }
  LOG_DEBUG("after inner_get_next_row",
      "total_size",
      this->local_allocator_.get_arena().total(),
      "used_size",
      this->local_allocator_.get_arena().used());
  return ret;
}

int ObWindowFunctionOp::copy_datum_row(
    const ObRADatumStore::StoredRow& row, ObWinbufPieceMsg& piece, int64_t buf_len, char* buf)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(buf));
  CK(buf_len > 0);
  CK(row.row_size_ > sizeof(ObRADatumStore::StoredRow));
  if (OB_SUCC(ret)) {
    piece.datum_row_ = new (buf) ObChunkDatumStore::StoredRow();
    piece.datum_row_->row_size_ = row.row_size_;
    piece.datum_row_->cnt_ = row.cnt_;
    MEMCPY(piece.datum_row_->payload_, row.payload_, row.row_size_ - sizeof(ObRADatumStore::StoredRow));
    char* base = const_cast<char*>(row.payload_);
    piece.datum_row_->unswizzling(base);
    piece.datum_row_->swizzling(piece.datum_row_->payload_);
  }
  return ret;
}

// send piece msg and wait whole msg.
int ObWindowFunctionOp::get_whole_msg(bool is_end, ObWinbufWholeMsg& whole, const ObRADatumStore::StoredRow* res_row)
{
  int ret = OB_SUCCESS;
  const ObWinbufWholeMsg* temp_whole_msg = NULL;
  ObPxSqcHandler* handler = ctx_.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parallel winbuf only supported in parallel execution mode", K(MY_SPEC.is_parallel_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "parallel winbuf in non-px mode");
  } else {
    ObPxSQCProxy& proxy = handler->get_sqc_proxy();
    ObWinbufPieceMsg piece;
    piece.op_id_ = MY_SPEC.id_;
    piece.thread_id_ = GETTID();
    piece.dfo_id_ = proxy.get_dfo_id();
    piece.is_datum_ = true;
    piece.col_count_ = res_row ? res_row->cnt_ : 0;
    if (is_end) {
      piece.is_end_ = true;
    } else if (OB_ISNULL(res_row)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("row ptr is null", K(ret));
    } else {
      piece.row_size_ = res_row->row_size_;
      piece.is_end_ = false;
      piece.payload_len_ = res_row->row_size_ - sizeof(ObChunkDatumStore::StoredRow);
      int64_t len = res_row->row_size_;
      char* buf = NULL;
      if (OB_ISNULL(buf = (char*)ctx_.get_allocator().alloc(len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc row buf", K(ret));
      } else if (OB_FAIL(copy_datum_row(*res_row, piece, len, buf))) {
        LOG_WARN("fail to deep copy row", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(proxy.get_dh_msg(
              MY_SPEC.id_, piece, temp_whole_msg, ctx_.get_physical_plan_ctx()->get_timeout_timestamp()))) {
        LOG_WARN("fail get win buf msg", K(ret));
      } else if (OB_ISNULL(temp_whole_msg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("whole msg is unexpected", K(ret));
      } else if (OB_FAIL(whole.assign(*temp_whole_msg))) {
        LOG_WARN("fail to assign msg", K(ret));
      }
    }
  }

  return ret;
}

int ObWindowFunctionOp::parallel_winbuf_process()
{
  int ret = OB_SUCCESS;
  WinFuncCell& wf_cell = *wf_list_.get_last();
  const ObRADatumStore::StoredRow* res_row = NULL;
  ObWinbufWholeMsg whole;
  if (wf_cell.part_rows_store_.count() <= 0) {
    if (OB_FAIL(get_whole_msg(true, whole))) {
      LOG_WARN("fail to get whole msg", K(ret));
    }
  } else if (OB_FAIL(wf_cell.part_rows_store_.get_row(0, res_row))) {
    LOG_WARN("get row failed", K(ret), K(res_row));
  } else if (OB_FAIL(get_whole_msg(false, whole, res_row))) {
    LOG_WARN("fail to get whole msg", K(ret));
  } else if (whole.is_empty_) {
    /*do nothing*/
  } else {
    ObChunkDatumStore::Iterator row_store_it;
    ObRADatumStore::StoredRow* new_row = const_cast<ObRADatumStore::StoredRow*>(res_row);
    const ObChunkDatumStore::StoredRow* row = NULL;
    bool is_first = true;
    if (OB_FAIL(whole.datum_store_.begin(row_store_it))) {
      LOG_WARN("failed to begin iterator for chunk row store", K(ret));
    } else {
      ObArray<ObDatumCmpFuncType> cmp_funcs;
      DLIST_FOREACH_X(wf_node, wf_list_, OB_SUCC(ret))
      {
        if (wf_node->wf_info_.func_type_ == T_FUN_MAX || wf_node->wf_info_.func_type_ == T_FUN_MIN) {
          ObDatumCmpFuncType cmp_func;
          cmp_func = ObDatumFuncs::get_nullsafe_cmp_func(wf_node->wf_info_.expr_->datum_meta_.type_,
              wf_node->wf_info_.expr_->datum_meta_.type_,
              NULL_LAST,
              wf_node->wf_info_.expr_->datum_meta_.cs_type_,
              share::is_oracle_mode());
          if (OB_FAIL(cmp_funcs.push_back(cmp_func))) {
            LOG_WARN("fail to push back cmp func", K(ret));
          }
        }
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(row_store_it.get_next_row(row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else if (is_first) {
          is_first = false;
          for (int i = 0; i < new_row->cnt_; ++i) {
            new_row->cells()[i] = row->cells()[i];
          }
        } else {
          int idx = 0;
          int64_t cmp_index = 0;
          DLIST_FOREACH_X(wf_node, wf_list_, OB_SUCC(ret))
          {
            switch (wf_node->wf_info_.func_type_) {
              case T_FUN_SUM:
              case T_FUN_COUNT: {
                ObDatum& l_datum = new_row->cells()[idx];
                const ObDatum& r_datum = row->cells()[idx];
                ObObjTypeClass tc = ob_obj_type_class(wf_node->wf_info_.expr_->datum_meta_.type_);
                if (OB_FAIL(ObAggregateCalcFunc::add_calc(l_datum, r_datum, l_datum, tc, ctx_.get_allocator()))) {
                  LOG_WARN("fail to add calc", K(ret));
                }
                break;
              }
              case T_FUN_MAX: {
                ObDatum& l_datum = new_row->cells()[idx];
                const ObDatum& r_datum = row->cells()[idx];
                ObDatumCmpFuncType cmp_func = cmp_funcs.at(cmp_index);
                // null-last cmp func should ignore null in max calc
                if (r_datum.is_null() && !l_datum.is_null()) {
                  /*do nothing*/
                } else if (!r_datum.is_null() && l_datum.is_null()) {
                  l_datum = r_datum;
                } else if (cmp_func(l_datum, r_datum) < 0) {
                  l_datum = r_datum;
                }
                cmp_index++;
                break;
              }
              case T_FUN_MIN: {
                ObDatum& l_datum = new_row->cells()[idx];
                const ObDatum& r_datum = row->cells()[idx];
                ObDatumCmpFuncType cmp_func = cmp_funcs.at(cmp_index);
                // null-last cmp func no need null special calc in min calc
                if (cmp_func(l_datum, r_datum) > 0) {
                  l_datum = r_datum;
                }
                cmp_index++;
                break;
              }
              default: {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("func is not supported", K(ret));
                break;
              }
            }
            idx++;
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        for (int i = 0; i < new_row->cnt_ && OB_SUCC(ret); ++i) {
          OZ(new_row->cells()[i].deep_copy(new_row->cells()[i], ctx_.get_allocator()));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    finish_parallel_ = true;
  }
  return ret;
}

int ObWindowFunctionOp::get_pos(RowsReader& row_reader, WinFuncCell& wf_cell, const int64_t row_idx,
    const ObRADatumStore::StoredRow& row, const bool is_upper, int64_t& pos, bool& got_null_val)
{
  const bool is_rows = WINDOW_ROWS == wf_cell.wf_info_.win_type_;
  const bool is_preceding = (is_upper ? wf_cell.wf_info_.upper_.is_preceding_ : wf_cell.wf_info_.lower_.is_preceding_);
  const bool is_unbounded = (is_upper ? wf_cell.wf_info_.upper_.is_unbounded_ : wf_cell.wf_info_.lower_.is_unbounded_);
  const bool is_nmb_literal =
      (is_upper ? wf_cell.wf_info_.upper_.is_nmb_literal_ : wf_cell.wf_info_.lower_.is_nmb_literal_);
  ObExpr* between_value_expr =
      (is_upper ? wf_cell.wf_info_.upper_.between_value_expr_ : wf_cell.wf_info_.lower_.between_value_expr_);

  LOG_DEBUG("get_pos",
      K(is_rows),
      K(is_upper),
      K(is_preceding),
      K(is_unbounded),
      K(is_nmb_literal),
      K(row_idx),
      KPC(between_value_expr),
      "part first row",
      wf_cell.part_first_row_idx_,
      "part end row",
      rows_store_.count() - 1,
      "order by cnt",
      wf_cell.wf_info_.sort_exprs_.count());
  int ret = OB_SUCCESS;
  pos = -1;
  got_null_val = false;
  if (NULL == between_value_expr && is_unbounded) {
    // no care rows or range
    pos = is_preceding ? wf_cell.part_first_row_idx_ : get_part_end_idx();
  } else if (NULL == between_value_expr && !is_unbounded) {
    // current row
    if (is_rows) {
      pos = row_idx;
    } else {
      // range
      // for current row, it's no sense for is_preceding
      // we should jump to detect step by step(for case that the sort columns has very small ndv)
      // @TODO: mark detected pos to use for next row

      // Exponential detection
      pos = row_idx;
      int step = 1;
      ObSortCollations& sort_collations = wf_cell.wf_info_.sort_collations_;
      ObSortFuncs& sort_cmp_funcs = wf_cell.wf_info_.sort_cmp_funcs_;
      const ObRADatumStore::StoredRow* a_row = NULL;
      while (OB_SUCC(ret)) {
        bool match = false;
        is_upper ? (pos -= step) : (pos += step);
        const bool overflow = is_upper ? (pos < wf_cell.part_first_row_idx_) : (pos > get_part_end_idx());
        if (overflow) {
          match = true;
        } else if (OB_FAIL(row_reader.get_row(pos, a_row))) {
          LOG_WARN("failed to get row", K(pos), K(ret));
        } else {
          for (int64_t i = 0; i < sort_collations.count() && !match; i++) {
            ObDatumCmpFuncType cmp_func = sort_cmp_funcs.at(i).cmp_func_;
            const int64_t idx = sort_collations.at(i).field_idx_;
            const ObDatum& l_datum = a_row->cells()[idx];
            const ObDatum& r_datum = row.cells()[idx];
            match = (0 != cmp_func(l_datum, r_datum));
            LOG_DEBUG("cmp ", K(idx), K(l_datum), K(r_datum), K(match), K(step), K(is_upper), K(pos));
          }
        }

        if (match) {
          is_upper ? (pos += step) : (pos -= step);
          if (1 == step) {
            break;
          } else {
            step = 1;
          }
        } else {
          step *= 2;
        }
      }  // end of while
    }
  } else {
    int64_t interval = 0;
    bool is_null = false;
    if (is_nmb_literal) {
      if (OB_ISNULL(between_value_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("between_value_expr is unexpected", KPC(between_value_expr), K(ret));
      } else if (OB_FAIL(get_param_int_value(*between_value_expr, eval_ctx_, is_null, interval))) {
        LOG_WARN("get_param_int_value failed", K(ret), KPC(between_value_expr));
      } else if (is_null) {
        got_null_val = true;
      } else if (interval < 0) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("invalid argument", K(ret), K(interval));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_rows) {
      // range or rows with expr
      pos = is_preceding ? row_idx - interval : row_idx + interval;
    } else if (wf_cell.wf_info_.sort_exprs_.count() == 0) {
      pos = is_preceding ? wf_cell.part_first_row_idx_ : get_part_end_idx();
    } else if (wf_cell.wf_info_.sort_exprs_.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only need one sort_exprs", K(ret));
    } else {
      const bool is_ascending_ = wf_cell.wf_info_.sort_collations_.at(0).is_ascending_;
      const int64_t cell_idx = wf_cell.wf_info_.sort_collations_.at(0).field_idx_;

      const static int L = 1;
      const static int LE = 1 << 1;
      const static int G = 1 << 2;
      const static int GE = 1 << 3;
      const static int ROLL = L | G;

      int cmp_mode = !(is_preceding ^ is_ascending_) ? L : G;
      if (is_preceding ^ is_upper) {
        cmp_mode = cmp_mode << 1;
      }
      pos = row_idx;
      bool re_direction = false;
      int step = cmp_mode & ROLL ? 1 : 0;
      int64_t cmp_times = 0;
      ObDatum* cmp_val = NULL;

      ObExpr* bound_expr =
          (is_upper ? wf_cell.wf_info_.upper_.range_bound_expr_ : wf_cell.wf_info_.lower_.range_bound_expr_);
      if (OB_ISNULL(bound_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(bound_expr));
      } else if (OB_FAIL(bound_expr->eval(eval_ctx_, cmp_val))) {
        LOG_WARN("calc compare value failed", K(ret));
      }
      ObSortFuncs& sort_cmp_funcs = wf_cell.wf_info_.sort_cmp_funcs_;
      ObDatumCmpFuncType cmp_func = sort_cmp_funcs.at(0).cmp_func_;
      while (OB_SUCC(ret)) {
        cmp_times++;
        ObDatum cur_val;
        bool match = false;
        const ObRADatumStore::StoredRow *a_row = NULL;
        (is_preceding ^ re_direction) ? (pos -= step) : (pos += step);
        const bool overflow = 
            (is_preceding ^ re_direction) ? pos < wf_cell.part_first_row_idx_ : pos > get_part_end_idx();
        if (overflow) {
          match = true;
        } else if (OB_FAIL(row_reader.get_row(pos, a_row))) {
          LOG_WARN("failed to get row", K(ret), K(pos));
        } else if (FALSE_IT(cur_val = a_row->cells()[cell_idx])) {
          // will not reach here
        } else {
          int cmp_result = cmp_func(cur_val, *cmp_val);
          match = ((cmp_mode & L) && cmp_result < 0) || ((cmp_mode & LE) && cmp_result <= 0) ||
                  ((cmp_mode & G) && cmp_result > 0) || ((cmp_mode & GE) && cmp_result >= 0);

          ObToStringDatum cur_val1(*bound_expr, cur_val);
          ObToStringDatum cmp_val1(*bound_expr, *cmp_val);
          LOG_DEBUG("cmp result",
              K(pos),
              K(cell_idx),
              K(cmp_times),
              K(cur_val),
              KPC(cmp_val),
              K(cmp_mode),
              K(cmp_result),
              K(match),
              "cur_val1",
              ObToStringDatum(*bound_expr, cur_val),
              "cmp_val1",
              ObToStringDatum(*bound_expr, *cmp_val));
        }

        if (OB_SUCC(ret)) {
          if (match) {
            if (pos == row_idx && !(cmp_mode & ROLL)) {
              // for LE/GE, if equal to current row,
              // change cmp_mode to search opposite direction.
              if (LE == cmp_mode) {
                cmp_mode = G;
              } else if (GE == cmp_mode) {
                cmp_mode = L;
              }
              re_direction = true;
              step = 1;
            } else if (step <= 1) {
              if (cmp_mode & ROLL) {
                (is_preceding ^ re_direction) ? pos += step : pos -= step;
              }
              break;
            } else {
              (is_preceding ^ re_direction) ? pos += step : pos -= step;
              step = 1;
            }
          } else {
            step = 0 == step ? 1 : (2 * step);
          }
        }
      }  // end of while
    }
  }
  return ret;
}

int ObWindowFunctionOp::collect_result(const int64_t idx, ObDatum& in_datum, WinFuncCell& wf_cell)
{
  int ret = OB_SUCCESS;
  const ObRADatumStore::StoredRow* prev_stored_row = NULL;
  int64_t result_datum_cnt = 0;
  if (OB_UNLIKELY(curr_row_collect_values_.count() != wf_list_.get_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("size is not equal", K(curr_row_collect_values_.count()), K(wf_list_.get_size()), K(ret));
  } else {
    if (wf_cell.get_prev() != wf_list_.get_header()) {
      WinFuncCell* prev_wf_cell = wf_cell.get_prev();
      result_datum_cnt += prev_wf_cell->wf_idx_;
      if (OB_FAIL(prev_wf_cell->part_rows_store_.get_row(idx, prev_stored_row))) {
        LOG_WARN("failed to get row", K(ret), K(idx), KPC(prev_wf_cell));
      } else if (OB_ISNULL(prev_stored_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("prev_stored_row is null", K(idx), K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < prev_stored_row->cnt_; ++i) {
          ObDatum& last_value = curr_row_collect_values_.at(i);
          last_value = prev_stored_row->cells()[i];
        }
      }
    }
    curr_row_collect_values_.at(result_datum_cnt++) = in_datum;
  }

  if (OB_SUCC(ret)) {
    const ObArrayHelper<ObDatum> tmp_array(result_datum_cnt, curr_row_collect_values_.get_data(), result_datum_cnt);
    if (OB_FAIL(wf_cell.part_rows_store_.add_row(tmp_array))) {
      LOG_WARN("add row failed", K(ret));
    } else {
      LOG_DEBUG("succ to collect_result", K(idx), K(in_datum), KPC(prev_stored_row), K(tmp_array), K(wf_cell));
    }
  }
  return ret;
}

int ObWindowFunctionSpec::register_to_datahub(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (is_parallel_) {
    if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else {
      void* buf = ctx.get_allocator().alloc(sizeof(ObWinbufWholeMsg::WholeMsgProvider));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObWinbufWholeMsg::WholeMsgProvider* provider = new (buf) ObWinbufWholeMsg::WholeMsgProvider();
        ObSqcCtx& sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
        if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(), *provider))) {
          LOG_WARN("fail add whole msg provider", K(ret));
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
