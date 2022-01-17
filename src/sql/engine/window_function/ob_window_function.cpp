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
#include "sql/engine/window_function/ob_window_function.h"
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

#define ADD_LEN_SQL_EXPRESSION(sql_expr) \
  {                                      \
    bool is_null = NULL == sql_expr;     \
    OB_UNIS_ADD_LEN(is_null);            \
    if (!is_null) {                      \
      OB_UNIS_ADD_LEN(*sql_expr);        \
    }                                    \
  }

#define SERIALIZE_SQL_EXPRESSION(sql_expr) \
  {                                        \
    bool is_null = NULL == sql_expr;       \
    OB_UNIS_ENCODE(is_null);               \
    if (!is_null) {                        \
      OB_UNIS_ENCODE(*sql_expr);           \
    }                                      \
  }

#define DESERIALIZE_SQL_EXPRESSION(sql_expr)                                     \
  {                                                                              \
    sql_expr = NULL;                                                             \
    bool is_null = false;                                                        \
    OB_UNIS_DECODE(is_null);                                                     \
    if (OB_SUCC(ret) && !is_null) {                                              \
      if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, sql_expr))) { \
        LOG_WARN("make sql expr failed", K(ret));                                \
      } else if (OB_ISNULL(sql_expr)) {                                          \
        ret = OB_ERR_UNEXPECTED;                                                 \
        LOG_WARN("NULL ptr", K(sql_expr), K(ret));                               \
      } else if (OB_FAIL(sql_expr->deserialize(buf, data_len, pos))) {           \
        LOG_WARN("deserialize expr failed", K(ret));                             \
      }                                                                          \
    }                                                                            \
  }

OB_DEF_SERIALIZE(ExtBound)
{
  int ret = OB_SUCCESS;

  OB_UNIS_ENCODE(is_preceding_);
  OB_UNIS_ENCODE(is_unbounded_);
  SERIALIZE_SQL_EXPRESSION(sql_expr_);
  for (int64_t i = 0; OB_SUCC(ret) && i < BOUND_EXPR_MAX; ++i) {
    SERIALIZE_SQL_EXPRESSION(sql_exprs_[i]);
  }
  OB_UNIS_ENCODE(is_nmb_literal_);
  return ret;
}

OB_DEF_DESERIALIZE(ExtBound)
{
  int ret = OB_SUCCESS;

  OB_UNIS_DECODE(is_preceding_);
  OB_UNIS_DECODE(is_unbounded_);
  DESERIALIZE_SQL_EXPRESSION(sql_expr_);
  for (int64_t i = 0; OB_SUCC(ret) && i < BOUND_EXPR_MAX; ++i) {
    DESERIALIZE_SQL_EXPRESSION(sql_exprs_[i]);
  }
  OB_UNIS_DECODE(is_nmb_literal_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ExtBound)
{
  int64_t len = 0;

  OB_UNIS_ADD_LEN(is_preceding_);
  OB_UNIS_ADD_LEN(is_unbounded_);
  ADD_LEN_SQL_EXPRESSION(sql_expr_);
  for (int64_t i = 0; i < BOUND_EXPR_MAX; ++i) {
    ADD_LEN_SQL_EXPRESSION(sql_exprs_[i]);
  }
  OB_UNIS_ADD_LEN(is_nmb_literal_);
  return len;
}

OB_DEF_SERIALIZE(FuncInfo)
{
  int ret = OB_SUCCESS;

  OB_UNIS_ENCODE(func_type_);
  SERIALIZE_SQL_EXPRESSION(aggr_column_);
  OB_UNIS_ENCODE(is_distinct_);
  OB_UNIS_ENCODE(is_ignore_null_);
  OB_UNIS_ENCODE(is_from_first_);
  int64_t N = params_.count();
  OB_UNIS_ENCODE(N);
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    OB_UNIS_ENCODE(*params_.at(i));
  }
  OB_UNIS_ENCODE(win_type_);
  OB_UNIS_ENCODE(upper_);
  OB_UNIS_ENCODE(lower_);
  OB_UNIS_ENCODE(result_index_);
  OB_UNIS_ENCODE(partition_cols_);
  OB_UNIS_ENCODE(sort_cols_);

  return ret;
}

OB_DEF_DESERIALIZE(FuncInfo)
{
  int ret = OB_SUCCESS;

  OB_UNIS_DECODE(func_type_);
  DESERIALIZE_SQL_EXPRESSION(aggr_column_);
  OB_UNIS_DECODE(is_distinct_);
  OB_UNIS_DECODE(is_ignore_null_);
  OB_UNIS_DECODE(is_from_first_);
  int64_t N = 0;
  OB_UNIS_DECODE(N);
  if (OB_FAIL(params_.init(N))) {
    LOG_WARN("failed to init params.", K(ret));
  } else { /*do nothing.*/
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    ObSqlExpression* param_expr = NULL;
    if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, param_expr))) {
      LOG_WARN("make sql expr failed", K(ret));
    } else if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(param_expr), K(ret));
    } else if (OB_FAIL(param_expr->deserialize(buf, data_len, pos))) {
      LOG_WARN("deserialize expr failed", K(ret));
    } else if (OB_FAIL(params_.push_back(param_expr))) {
      LOG_WARN("push back expr failed", K(ret));
    }
  }
  OB_UNIS_DECODE(win_type_);
  upper_.my_phy_plan_ = my_phy_plan_;
  lower_.my_phy_plan_ = my_phy_plan_;
  OB_UNIS_DECODE(upper_);
  OB_UNIS_DECODE(lower_);
  OB_UNIS_DECODE(result_index_);
  OB_UNIS_DECODE(partition_cols_);
  OB_UNIS_DECODE(sort_cols_);

  return ret;
}

OB_DEF_SERIALIZE_SIZE(FuncInfo)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(func_type_);
  ADD_LEN_SQL_EXPRESSION(aggr_column_);
  OB_UNIS_ADD_LEN(is_distinct_);
  OB_UNIS_ADD_LEN(is_ignore_null_);
  OB_UNIS_ADD_LEN(is_from_first_);
  int64_t N = params_.count();
  OB_UNIS_ADD_LEN(N);
  for (int64_t i = 0; i < N; ++i) {
    OB_UNIS_ADD_LEN(*params_.at(i));
  }
  OB_UNIS_ADD_LEN(win_type_);
  OB_UNIS_ADD_LEN(upper_);
  OB_UNIS_ADD_LEN(lower_);
  OB_UNIS_ADD_LEN(result_index_);
  OB_UNIS_ADD_LEN(partition_cols_);
  OB_UNIS_ADD_LEN(sort_cols_);
  return len;
}

ObWindowFunction::ObWindowFunctionCtx::ObWindowFunctionCtx(ObExecContext& ctx)
    : ObPhyOperatorCtx(ctx),
      next_part_first_row_(NULL),
      last_output_row_(-1),
      op_(NULL),
      all_first_(false),
      rescan_(false),
      finish_parallel_(false)
{
  reset();
}

int ObWindowFunction::ObWindowFunctionCtx::update_next_part_first_row(const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (NULL == next_part_first_row_) {
    ret = Utils::copy_new_row(local_allocator_, row, next_part_first_row_);
  } else {
    // for (int64_t i = 0; OB_SUCC(ret) && i < row.get_count(); ++i) {
    //  ret = Utils::clone_cell(local_allocator_, row.get_cell(i), next_part_first_row_->get_cell(i));
    //}
    int64_t size = sizeof(ObNewRow) + next_part_first_row_->get_deep_copy_size();
    const int64_t need_size = sizeof(ObNewRow) + row.get_deep_copy_size();
    char* buf = reinterpret_cast<char*>(next_part_first_row_);
    if (size < need_size) {
      size = std::max(2 * size, need_size);
      if (NULL == (buf = static_cast<char*>(local_allocator_.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    if (OB_SUCC(ret)) {
      ObNewRow* dst = new (buf) ObNewRow();
      int64_t pos = sizeof(ObNewRow);
      if (OB_FAIL(dst->deep_copy(row, buf, size, pos))) {
        LOG_WARN("copy row failed", K(ret));
      } else {
        next_part_first_row_ = dst;
      }
    }
  }
  return ret;
}

ObWindowFunction::AggFunc::~AggFunc()
{
  DLIST_FOREACH_NORET(aggr_column, aggr_columns_)
  {
    if (aggr_column != NULL) {
      aggr_column->~ObSqlExpression();
    }
  }
}

int ObWindowFunction::AggFunc::init_all_first()
{
  int ret = OB_SUCCESS;
  void* tmp_ptr = func_ctx_->w_ctx_->local_allocator_.alloc(sizeof(ObAggregateExpression));
  if (NULL == tmp_ptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObAggregateExpression* aggr_column = new (tmp_ptr) ObAggregateExpression(func_ctx_->w_ctx_->local_allocator_);
    if (OB_FAIL(aggr_column->assign(*func_ctx_->func_info_->aggr_column_))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(ObSqlExpressionUtil::add_expr_to_list(aggr_columns_, aggr_column))) {
      LOG_WARN("add failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t child_column_count = (func_ctx_->w_ctx_->op_->child_op_->get_projector_size() > 0
                                            ? func_ctx_->w_ctx_->op_->child_op_->get_projector_size()
                                            : func_ctx_->w_ctx_->op_->child_op_->get_column_count());
    if (OB_FAIL(aggr_func_.init(child_column_count, &aggr_columns_, func_ctx_->w_ctx_->expr_ctx_, 1024, 128))) {
      LOG_WARN("failed to init agg func", K(ret));
    } else {
      result_index_ = child_column_count;
      result_ = NULL;
      aggr_func_.set_in_window_func();
    }
  }
  return ret;
}

void ObWindowFunction::AggFunc::reset_for_restart_self()
{
  aggr_func_.reuse();
  prepare_ = true;
}

int ObWindowFunction::AggFunc::trans_self(const common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (prepare_) {
    if ((OB_FAIL(aggr_func_.prepare(row)))) {
      LOG_WARN("fail to prepare the aggr func", K(ret), K(row));
    } else {
      prepare_ = false;
    }
  } else {
    ObSQLSessionInfo* my_session = func_ctx_->w_ctx_->exec_ctx_.get_my_session();
    const ObTimeZoneInfo* tz_info = (NULL != my_session) ? my_session->get_timezone_info() : NULL;
    if (OB_FAIL(aggr_func_.process(row, tz_info))) {
      LOG_WARN("fail to process the aggr func", K(ret), K(row));
    }
  }
  if (OB_SUCC(ret)) {
    // uppon invoke trans(), forbiden it to reuse the last_result
    result_ = NULL;
  }
  return ret;
}

int ObWindowFunction::AggFunc::final(ObObj& val)
{
  // @TODO: optimiation
  int ret = OB_SUCCESS;
  if (NULL == result_) {
    ObSQLSessionInfo* my_session = func_ctx_->w_ctx_->exec_ctx_.get_my_session();
    const ObTimeZoneInfo* tz_info = (NULL != my_session) ? my_session->get_timezone_info() : NULL;
    ret = aggr_func_.get_result(func_ctx_->w_ctx_->get_cur_row(), tz_info);
    if (OB_FAIL(ret)) {
    } else {
      // val = func_ctx_->w_ctx_->get_cur_row().cells_[func_ctx_->func_info_->result_index_];
      // we should use result index of aggr
      result_buf_ = func_ctx_->w_ctx_->get_cur_row().cells_[result_index_];
      result_ = &result_buf_;
    }
  }
  if (OB_SUCC(ret)) {
    val = *result_;
  }
  return ret;
}

int ObWindowFunction::NonAggFuncRowNumber::eval(
    RowReader& assist_reader, const int64_t row_idx, const ObNewRow& row, const WinFrame& frame, ObObj& val)
{
  int ret = OB_SUCCESS;
  UNUSED(assist_reader);
  UNUSED(row_idx);
  UNUSED(row);
  UNUSED(frame);
  val.set_int(row_idx - frame.head_ + 1);
  return ret;
}

int ObWindowFunction::get_param_int_value(ObExprCtx& expr_ctx, const ObObj& tmp_obj, int64_t& value)
{
  int ret = OB_SUCCESS;
  if (tmp_obj.is_integer_type()) {
    value = tmp_obj.get_int();
  } else if (share::is_oracle_mode()) {
    number::ObNumber num;
    if (!tmp_obj.is_number()) {
      // It needs to be cast, and it is a waste of constructing and destructing ctx every line...
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_NUMBER_V2(tmp_obj, num);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to cast object", K(tmp_obj), K(ret));
      }
    } else {
      num = tmp_obj.get_number();
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(num.extract_valid_int64_with_trunc(value))) {
        LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid number", K(tmp_obj), K(ret));
  }
  return ret;
}

int ObWindowFunction::register_to_datahub(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (is_parallel_) {
    if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else {
      ObIAllocator& allocator = ctx.get_sqc_handler()->get_safe_allocator();
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

int ObWindowFunction::NonAggFuncNthValue::eval(
    RowReader& assist_reader, const int64_t row_idx, const ObNewRow& row, const WinFrame& frame, ObObj& val)
{
  UNUSED(row_idx);
  int ret = OB_SUCCESS;
  const ObIArray<ObSqlExpression*>& params = func_ctx_->func_info_->params_;
  if (OB_UNLIKELY(params.count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid number of params", K(params.count()), K(ret));
  } else {
    const bool is_ignore_null = func_ctx_->func_info_->is_ignore_null_;
    const bool is_from_first = func_ctx_->func_info_->is_from_first_;
    ObExprCtx& expr_ctx = func_ctx_->w_ctx_->expr_ctx_;
    int64_t nth_val = 0;
    ObObj tmp_obj;
    if (OB_FAIL(params.at(1)->calc(expr_ctx, row, tmp_obj))) {
      LOG_WARN("fail to calc result row", K(ret));
    } else if (OB_FAIL(ObWindowFunction::get_param_int_value(expr_ctx, tmp_obj, nth_val))) {
      LOG_WARN("fail to get param int value", K(ret));
    } else if (nth_val <= 0) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("invalid argument", K(ret), K(nth_val));
    }
    int64_t k = 0;
    bool is_calc_nth = false;
    for (int64_t i = is_from_first ? frame.head_ : frame.tail_;
         OB_SUCC(ret) && (is_from_first ? (i <= frame.tail_) : (i >= frame.head_));
         is_from_first ? ++i : --i) {
      const ObNewRow* a_row = NULL;
      if (OB_FAIL(assist_reader.at(i, a_row))) {
        LOG_WARN("failed to get row", K(ret), K(i));
      } else if (OB_FAIL(params.at(0)->calc(expr_ctx, *a_row, val))) {
        LOG_WARN("fail to calc result row", K(ret));
      } else if ((!val.is_null() || !is_ignore_null) && ++k == nth_val) {
        is_calc_nth = true;
        break;
      }
    }
    if (OB_SUCC(ret) && !is_calc_nth) {
      val.set_null();
    }
  }
  return ret;
}

void ObWindowFunction::NonAggFuncLeadOrLag::reset_for_restart_self()
{}

int ObWindowFunction::NonAggFuncLeadOrLag::eval(
    RowReader& assist_reader, const int64_t row_idx, const ObNewRow& row, const WinFrame& frame, ObObj& val)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObSqlExpression*>& params = func_ctx_->func_info_->params_;
  // LEAD provides access to a row at a given physical offset beyond that position
  // while LAG provides access to a row at a given physical offset prior to that position.
  const bool is_lead = T_WIN_FUN_LEAD == func_ctx_->func_info_->func_type_;
  int lead_lag_offset_direction = is_lead ? +1 : -1;
  // 0 -> value_expr 1 -> offset 2 -> default value
  ObObj lead_lag_params[3];
  enum LeadLagParamType { VALUE_EXPR = 0, OFFSET = 1, DEFAULT_VALUE = 2, NUM_LEAD_LAG_PARAMS };
  // if not specified, the default offset is 1.
  lead_lag_params[OFFSET].set_int(1);
  // if not specified, the default value is NULL.
  // the following line is not need since NULL is the default value of a new ObObj created by default ctor.
  // lead_lag_params[DEFAULT_VALUE].set_null();

  if (OB_UNLIKELY(params.count() > NUM_LEAD_LAG_PARAMS || params.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid number of params", K(ret));
  } else {
    ObExprCtx& expr_ctx = func_ctx_->w_ctx_->expr_ctx_;
    for (int64_t j = 0; OB_SUCC(ret) && j < params.count(); ++j) {
      if (OB_ISNULL(params.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param", K(ret));
      } else if (OB_FAIL(params.at(j)->calc(expr_ctx, row, lead_lag_params[j]))) {
        LOG_WARN("fail to calc result row", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t offset = 0;
      if (OB_FAIL(ObExprUtil::get_trunc_int64(lead_lag_params[OFFSET], expr_ctx, offset))) {
        LOG_WARN("get_trunc_int64 failed", K(ret));
      } else if (OB_UNLIKELY(offset < 0 ||
                             (share::is_oracle_mode() && func_ctx_->func_info_->is_ignore_null_ && offset == 0))) {
        ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
        LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, offset);
        LOG_WARN("lead/lag argument is out of range", K(ret), K(offset));
      } else { /* do nothing */
      }
      if (OB_SUCC(ret)) {
        int64_t step = 0;
        bool found = false;
        for (int64_t j = row_idx; OB_SUCC(ret) && !found && j >= frame.head_ && j <= frame.tail_;
             j += lead_lag_offset_direction) {
          const ObNewRow* a_row = NULL;
          if (OB_FAIL(assist_reader.at(j, a_row))) {
            LOG_WARN("failed to get row", K(ret), K(j));
          } else if (OB_FAIL(params.at(0)->calc(expr_ctx, *a_row, lead_lag_params[VALUE_EXPR]))) {
            LOG_WARN("fail to calc result row", K(ret));
          } else if (func_ctx_->func_info_->is_ignore_null_ && lead_lag_params[VALUE_EXPR].is_null()) {
            // When row_idx is null, step++ is omitted under non-ignore nulls;
            step = (j == row_idx) ? step + 1 : step;
          } else if (step++ == offset) {
            found = true;
            val = lead_lag_params[VALUE_EXPR];
          } else { /* do nothing. */
          }
        }
        if (OB_SUCC(ret) && !found) {
          val = lead_lag_params[DEFAULT_VALUE];
        }
      }
    }
  }
  return ret;
}

int ObWindowFunction::NonAggFuncNtile::eval(
    RowReader& assist_reader, const int64_t row_idx, const ObNewRow& row, const WinFrame& frame, ObObj& val)
{
  int ret = OB_SUCCESS;
  UNUSED(assist_reader);
  UNUSED(row);
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
  ObSqlExpression* param = NULL;
  ObObj tmp_obj;
  int64_t bucket_num = 0;
  const ObIArray<ObSqlExpression*>& params = func_ctx_->func_info_->params_;
  if (OB_UNLIKELY(params.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The number of arguments of NTILE should be 1", K(params.count()), K(ret));
  } else if (OB_ISNULL(param = params.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is NULL", K(ret));
  } else {
    if (OB_FAIL(param->calc(func_ctx_->w_ctx_->expr_ctx_, row, tmp_obj))) {
      LOG_WARN("failed to to calc result row", K(ret));
    } else if (tmp_obj.is_null()) {
      // return NULL when backets_num is NULL
      val.set_null();
    } else if (!tmp_obj.is_int() && !tmp_obj.is_uint64() && (share::is_mysql_mode() || !tmp_obj.is_number())) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("invalid argument", K(ret), K(tmp_obj));
    } else {
      // we restrict the bucket_num in range [0, (1<<63)-1]
      if (tmp_obj.is_uint64()) {
        uint64_t ubucket_num = 0;
        if (OB_FAIL(tmp_obj.get_uint64(ubucket_num))) {
          LOG_WARN("failed to get uint", K(ret), K(tmp_obj));
        } else if (ubucket_num > INT64_MAX) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("bucket_num out of range", K(ret), K(ubucket_num), K(INT64_MAX));
        } else {
          bucket_num = static_cast<int64_t>(ubucket_num);
        }
      } else if (tmp_obj.is_number()) {
        const number::ObNumber num = tmp_obj.get_number();
        if (OB_FAIL(num.extract_valid_int64_with_trunc(bucket_num))) {
          LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(num));
        }
      } else if (OB_FAIL(tmp_obj.get_int(bucket_num))) {
        LOG_WARN("failed to get int", K(ret), K(tmp_obj));
      }
      if (OB_SUCC(ret)) {
        if (bucket_num <= 0) {
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
          val.set_int(result);
          LOG_DEBUG("ntile print result", K(result), K(bucket_num));
        }
      }
    }
  }
  return ret;
}

void ObWindowFunction::NonAggFuncRankLike::reset_for_restart_self()
{
  rank_of_prev_row_ = 0;
}

int ObWindowFunction::NonAggFuncRankLike::eval(
    RowReader& assist_reader, const int64_t row_idx, const ObNewRow& row, const WinFrame& frame, ObObj& val)
{
  int ret = OB_SUCCESS;

  bool equal_with_prev_row = false;
  if (row_idx != frame.head_) {
    equal_with_prev_row = true;
    const ObIArray<ObSortColumn>& sort_cols = func_ctx_->func_info_->sort_cols_;
    const ObNewRow* tmp_row = NULL;
    if (OB_FAIL(assist_reader.at(row_idx - 1, tmp_row))) {
      LOG_WARN("failed to get row", K(ret), K(row_idx));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < sort_cols.count(); ++i) {
        const ObObj& prev_obj = tmp_row->get_cell(sort_cols.at(i).index_);
        const ObObj& cur_obj = row.get_cell(sort_cols.at(i).index_);
        if (!ObObjCmpFuncs::compare_oper_nullsafe(prev_obj, cur_obj, prev_obj.get_collation_type(), CO_EQ)) {
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
    } else if (T_WIN_FUN_RANK == func_ctx_->func_info_->func_type_ ||
               T_WIN_FUN_PERCENT_RANK == func_ctx_->func_info_->func_type_) {
      rank = row_idx - frame.head_ + 1;
    } else if (T_WIN_FUN_DENSE_RANK == func_ctx_->func_info_->func_type_) {
      // dense_rank
      rank = rank_of_prev_row_ + 1;
    }
    if (T_WIN_FUN_PERCENT_RANK == func_ctx_->func_info_->func_type_) {
      // result will be zero when only one row within frame
      if (0 == frame.tail_ - frame.head_) {
        number::ObNumber res_nmb;
        res_nmb.set_zero();
        val.set_number(res_nmb);
      } else {
        ObExprCtx& expr_ctx = func_ctx_->w_ctx_->expr_ctx_;
        number::ObNumber numerator;
        number::ObNumber denominator;
        number::ObNumber res_nmb;
        if (OB_FAIL(numerator.from(rank - 1, *expr_ctx.calc_buf_))) {
          LOG_WARN("failed to build number from int64_t", K(ret));
        } else if (OB_FAIL(denominator.from(frame.tail_ - frame.head_, *expr_ctx.calc_buf_))) {
          LOG_WARN("failed to build number from int64_t", K(ret));
        } else if (OB_FAIL(numerator.div(denominator, res_nmb, *expr_ctx.calc_buf_))) {
          LOG_WARN("failed to div number", K(ret));
        } else {
          val.set_number(res_nmb);
        }
      }
    } else {
      val.set_int(rank);
    }
    if (OB_SUCC(ret)) {
      rank_of_prev_row_ = rank;
    }
  }
  return ret;
}

int ObWindowFunction::NonAggFuncCumeDist::eval(
    RowReader& assist_reader, const int64_t row_idx, const ObNewRow& row, const WinFrame& frame, ObObj& val)
{
  int ret = OB_SUCCESS;

  int64_t same_idx = row_idx;
  bool should_continue = true;
  const ObNewRow& ref_row = row;
  const ObIArray<ObSortColumn>& sort_cols = func_ctx_->func_info_->sort_cols_;
  while (should_continue && OB_SUCC(ret) && same_idx < frame.tail_) {
    const ObNewRow* a_row = NULL;
    if (OB_FAIL(assist_reader.at(same_idx + 1, a_row))) {
      LOG_WARN("fail to get row", K(ret), K(same_idx));
    } else {
      const ObNewRow& iter_row = *a_row;
      for (int64_t j = 0; should_continue && OB_SUCC(ret) && j < sort_cols.count(); j++) {
        const ObObj& ref_obj = ref_row.get_cell(sort_cols.at(j).index_);
        const ObObj& iter_obj = iter_row.get_cell(sort_cols.at(j).index_);
        if (ObObjCmpFuncs::compare_oper_nullsafe(ref_obj, iter_obj, ref_obj.get_collation_type(), CO_NE)) {
          should_continue = false;
        }
      }
      if (OB_SUCC(ret) && should_continue) {
        ++same_idx;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObExprCtx& expr_ctx = func_ctx_->w_ctx_->expr_ctx_;
    // number of row[cur] >= row[:] (whether `>=` or other is depend on ORDER BY)
    number::ObNumber numerator;
    // total tuple of current window
    number::ObNumber denominator;
    // result number
    number::ObNumber res_nmb;

    if (OB_FAIL(numerator.from(same_idx - frame.head_ + 1, *expr_ctx.calc_buf_))) {
      LOG_WARN("failed to build number from int64_t", K(ret));
    } else if (OB_FAIL(denominator.from(frame.tail_ - frame.head_ + 1, *expr_ctx.calc_buf_))) {
      LOG_WARN("failed to build number from int64_t", K(ret));
    } else if (OB_FAIL(numerator.div(denominator, res_nmb, *expr_ctx.calc_buf_))) {
      LOG_WARN("failed to div number", K(ret));
    } else {
      val.set_number(res_nmb);
    }
  }

  return ret;
}

int ObWindowFunction::Utils::copy_new_row(ObIAllocator& allocator, const ObNewRow& src, ObNewRow*& dst)
{
  int ret = OB_SUCCESS;
  dst = NULL;

  char* buf = NULL;
  int64_t pos = sizeof(ObNewRow);
  int64_t size = sizeof(ObNewRow) + src.get_deep_copy_size();
  if (NULL == (buf = static_cast<char*>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc mem failed", K(size), K(ret));
  } else {
    dst = new (buf) ObNewRow();
    if (OB_FAIL(dst->deep_copy(src, buf, size, pos))) {
      LOG_WARN("copy row failed", K(ret));
    }
  }

  return ret;
}

int ObWindowFunction::Utils::clone_cell(ObIAllocator& allocator, const ObObj& cell, ObObj& cell_clone)
{
  int ret = OB_SUCCESS;
  ObObjType type = cell.get_type();
  if (OB_UNLIKELY(cell.need_deep_copy())) {
    char* buf = NULL;
    int64_t size = sizeof(int64_t) + cell.get_deep_copy_size();
    int64_t pos = 0;
    if (cell_clone.get_type() == type && (NULL != cell_clone.get_data_ptr() && 0 != cell_clone.get_data_length())) {
      buf = (char*)((int64_t*)(cell_clone.get_data_ptr()) - 1);
      if (size > *(int64_t*)buf) {
        size = size * 2;
        if (NULL != (buf = static_cast<char*>(allocator.alloc(size)))) {
          *((int64_t*)buf) = size;
        } else {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }
    } else {
      if (NULL != (buf = static_cast<char*>(allocator.alloc(size)))) {
        *((int64_t*)buf) = size;
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }

    if (OB_SUCC(ret)) {
      pos += sizeof(int64_t);
      ret = cell_clone.deep_copy(cell, buf, size, pos);
    }
  } else {
    cell_clone = cell;
  }

  return ret;
}

int ObWindowFunction::Utils::convert_stored_row(const ObNewRow& stored_row, ObNewRow& row)
{
  int ret = OB_SUCCESS;
  // stored_row comes from the child operator, so use the projected
  // row is generated by the current operator, so it cannot be taken by the projection of the current operator
  if (stored_row.get_count() > row.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cells count", K(stored_row.get_count()), K(row.count_), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stored_row.get_count(); ++i) {
    row.cells_[i] = stored_row.get_cell(i);
  }
  return ret;
}

int ObWindowFunction::Utils::check_same_partition(
    const ObIArray<ObColumnInfo>& partition_cols, const ObNewRow& row1, const ObNewRow& row2, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;

  const ObObj* lcell = NULL;
  const ObObj* rcell = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && is_same && i < partition_cols.count(); ++i) {
    int64_t partition_idx = partition_cols.at(i).index_;
    if (OB_UNLIKELY(partition_idx >= row1.get_count()) || OB_UNLIKELY(partition_idx >= row2.get_count()) ||
        OB_UNLIKELY(partition_idx < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(row1.get_count()), K(row2.get_count()), K(partition_idx), K(partition_cols));
    } else {
      lcell = &row1.get_cell(partition_idx);
      rcell = &row2.get_cell(partition_idx);  // read through projector
      if (lcell->compare(*rcell, partition_cols.at(i).cs_type_) != 0) {
        is_same = false;
      }
    }
  }

  return ret;
}

// todo: concern boundary
bool ObWindowFunction::Utils::need_restart_agg(
    const AggFunc& agg_func, const WinFrame& last_valid_frame, const WinFrame& new_frame)
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
    } else if (!agg_func.can_inv()) {
      // has sliding-out row
      if (new_frame.head_ > last_valid_frame.head_ || new_frame.tail_ < last_valid_frame.tail_) {
        need = true;
      }
    }
  }
  return need;
}

bool ObWindowFunction::Utils::valid_frame(const WinFrame& part_frame, const WinFrame& frame)
{
  return frame.head_ <= frame.tail_ && frame.head_ <= part_frame.tail_ && frame.tail_ >= part_frame.head_;
}

bool ObWindowFunction::Utils::same_frame(const WinFrame& left, const WinFrame& right)
{
  return left.head_ == right.head_ && left.tail_ == right.tail_;
}

void ObWindowFunction::Utils::prune_frame(const WinFrame& part_frame, WinFrame& frame)
{
  // it's caller's responsibility for invoking valid_frame() first
  if (frame.head_ < part_frame.head_) {
    frame.head_ = part_frame.head_;
  }
  if (frame.tail_ > part_frame.tail_) {
    frame.tail_ = part_frame.tail_;
  }
}

int ObWindowFunction::Utils::invoke_agg(AggFunc& agg_func, const bool use_trans, const common::ObNewRow& row)
{
  return use_trans ? agg_func.trans(row) : agg_func.inv_trans(row);
}

template <class FuncType>
int ObWindowFunction::FuncAllocer::alloc(BaseFunc*& return_func)
{
  int ret = OB_SUCCESS;
  void* tmp_ptr = local_allocator_->alloc(sizeof(FuncType));
  if (OB_ISNULL(tmp_ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    return_func = new (tmp_ptr) FuncType();
  }
  return ret;
}

ObWindowFunction::ObWindowFunction(ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc), func_infos_(alloc), is_parallel_(false)
{}

ObWindowFunction::~ObWindowFunction()
{}

void ObWindowFunction::reset()
{
  func_infos_.reset();
  ObSingleChildPhyOperator::reset();
}

void ObWindowFunction::reuse()
{
  reset();
}

int ObWindowFunction::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObWindowFunctionCtx* w_ctx = NULL;

  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else if (OB_ISNULL(w_ctx = GET_PHY_OPERATOR_CTX(ObWindowFunctionCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K_(id));
  } else if (func_infos_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cnt", K(func_infos_.count()), K(ret));
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else {
    const uint64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
    ret = w_ctx->rw_.reset(tenant_id);
    if (OB_SUCC(ret)) {
      w_ctx->reset();
      w_ctx->op_ = const_cast<ObWindowFunction*>(this);
      ret = w_ctx->init();
    }
  }
  return ret;
}

int ObWindowFunction::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObWindowFunctionCtx* w_ctx = NULL;
  if (OB_FAIL(ObSingleChildPhyOperator::rescan(ctx))) {
    LOG_WARN("rescan child operator failed", K(ret));
  } else if (OB_ISNULL(w_ctx = GET_PHY_OPERATOR_CTX(ObWindowFunctionCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("distinct ctx is null");
  } else if (OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else {
    const uint64_t tenant_id = ctx.get_my_session()->get_effective_tenant_id();
    ret = w_ctx->rw_.reset(tenant_id);
    if (OB_SUCC(ret)) {
      w_ctx->next_part_first_row_ = NULL;
      w_ctx->last_output_row_ = -1;
      w_ctx->rescan_ = true;
      w_ctx->finish_parallel_ = false;
    }
  }
  return ret;
}

int ObWindowFunction::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObWindowFunctionCtx* w_ctx = NULL;

  if (OB_ISNULL(w_ctx = GET_PHY_OPERATOR_CTX(ObWindowFunctionCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    // free some
    DLIST_FOREACH_NORET(func, w_ctx->func_list_)
    {
      if (func != NULL) {
        FuncCtx* func_ctx = func->func_ctx_;
        if (func_ctx != NULL) {
          func_ctx->~FuncCtx();
        }
        func->~BaseFunc();
      }
    }
  }
  return ret;
}

int ObWindowFunction::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObWindowFunctionCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("failed to create ctx", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("init current row failed", K(ret));
  }
  return ret;
}

int ObWindowFunction::ObWindowFunctionCtx::input_first_row()
{
  int ret = OB_SUCCESS;

  const ObNewRow* input_row = NULL;
  if (OB_FAIL(op_->child_op_->get_next_row(exec_ctx_, input_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("child_op failed to get next row", K(ret));
    } else if (op_->is_parallel_ && !finish_parallel_ && OB_FAIL(parallel_winbuf_process())) {
      LOG_WARN("fail to parallel process window function buffer", K(ret));
    }
  } else if (OB_ISNULL(input_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(input_row), K(ret));
  } else if (OB_FAIL(update_next_part_first_row(*input_row))) {
    LOG_WARN("update failed", K(*input_row), K(ret));
  }
  return ret;
}

int ObWindowFunction::ObWindowFunctionCtx::input_one_row(FuncCtx* func_ctx)
{
  int ret = OB_SUCCESS;

  const ObNewRow* input_row = NULL;
  if (OB_FAIL(op_->child_op_->get_next_row(exec_ctx_, input_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("child_op failed to get next row", K(ret));
    } else {
      func_ctx->part_iter_end_ = true;
      next_part_first_row_ = NULL;
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(input_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(input_row), K(ret));
  } else {
    bool same_part = false;
    const ObNewRow* a_row = NULL;
    if (OB_FAIL(rw_.at(func_ctx->part_first_row_, a_row))) {
      LOG_WARN("get row failed", K(ret), K(func_ctx->part_first_row_));
    } else if (OB_FAIL(
                   Utils::check_same_partition(func_ctx->func_info_->partition_cols_, *a_row, *input_row, same_part))) {
      LOG_WARN("check same partition failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (!same_part) {
        func_ctx->part_iter_end_ = true;
        if (OB_FAIL(update_next_part_first_row(*input_row))) {
          LOG_WARN("update failed", K(*input_row), K(ret));
        }
      } else {
        if (OB_FAIL(rw_.add_row(*input_row))) {
          LOG_WARN("add row failed", K(*input_row), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObWindowFunction::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;

  ObWindowFunctionCtx* w_ctx = NULL;
  if (OB_ISNULL(w_ctx = GET_PHY_OPERATOR_CTX(ObWindowFunctionCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator ctx failed");
  } else {
    ret = w_ctx->get_next_row(row);
  }
  return ret;
}

int ObWindowFunction::ObWindowFunctionCtx::compute(
    RowReader& assist_reader, BaseFunc* func, const int64_t row_idx, ObObj& val)
{
  int ret = OB_SUCCESS;

  FuncCtx* func_ctx = func->func_ctx_;
  AggFunc* agg_func = NULL;
  NonAggFunc* non_agg_func = NULL;
  const bool is_agg = func->is_agg();
  const ObNewRow* row = NULL;

  if (is_agg) {
    agg_func = static_cast<AggFunc*>(func);
  } else {
    non_agg_func = static_cast<NonAggFunc*>(func);
  }
  WinFrame new_frame;
  bool upper_has_null = false;
  bool lower_has_null = false;
  if (OB_FAIL(rw_.at(row_idx, row))) {
    LOG_WARN("failed to get row", K(ret), K(row_idx));
  } else if (OB_FAIL(get_pos(assist_reader,
                 func_ctx,
                 row_idx,
                 *row,
                 WINDOW_ROWS == func_ctx->func_info_->win_type_,
                 true,
                 func_ctx->func_info_->upper_.is_preceding_,
                 func_ctx->func_info_->upper_.is_unbounded_,
                 func_ctx->func_info_->upper_.is_nmb_literal_,
                 func_ctx->func_info_->upper_.sql_expr_,
                 func_ctx->func_info_->upper_.sql_exprs_,
                 new_frame.head_,
                 upper_has_null))) {
    LOG_WARN("get pos failed", K(ret));
  } else if (OB_FAIL(get_pos(assist_reader,
                 func_ctx,
                 row_idx,
                 *row,
                 WINDOW_ROWS == func_ctx->func_info_->win_type_,
                 false,
                 func_ctx->func_info_->lower_.is_preceding_,
                 func_ctx->func_info_->lower_.is_unbounded_,
                 func_ctx->func_info_->lower_.is_nmb_literal_,
                 func_ctx->func_info_->lower_.sql_expr_,
                 func_ctx->func_info_->lower_.sql_exprs_,
                 new_frame.tail_,
                 lower_has_null))) {
    LOG_WARN("get pos failed", K(ret));
  } else {
    WinFrame& last_valid_frame = func->last_valid_frame_;
    // The part_frame here is only used for cropping, only the upper boundary is accurate
    WinFrame part_frame(func_ctx->part_first_row_, rw_.count() - 1);
    LOG_DEBUG("dump frame",
        K(part_frame),
        K(last_valid_frame),
        K(new_frame),
        K(upper_has_null),
        K(lower_has_null),
        K(rw_.count()),
        K(row_idx),
        K(row));
    if (!upper_has_null && !lower_has_null && Utils::valid_frame(part_frame, new_frame)) {
      Utils::prune_frame(part_frame, new_frame);
      if (is_agg) {
        const ObNewRow* cur_row = NULL;
        if (!Utils::same_frame(last_valid_frame, new_frame)) {
          if (!Utils::need_restart_agg(*agg_func, last_valid_frame, new_frame)) {
            bool use_trans = new_frame.head_ < last_valid_frame.head_;
            int64_t b = min(new_frame.head_, last_valid_frame.head_);
            int64_t e = max(new_frame.head_, last_valid_frame.head_);
            for (int64_t i = b; OB_SUCC(ret) && i < e; ++i) {
              if (OB_FAIL(rw_.at(i, cur_row))) {
                LOG_WARN("get cur row failed", K(ret), K(i));
              } else if (OB_FAIL(Utils::invoke_agg(*agg_func, use_trans, *cur_row))) {
                LOG_WARN("invoke failed", K(use_trans), K(ret));
              }
            }
            use_trans = new_frame.tail_ > last_valid_frame.tail_;
            b = min(new_frame.tail_, last_valid_frame.tail_);
            e = max(new_frame.tail_, last_valid_frame.tail_);
            for (int64_t i = b + 1; OB_SUCC(ret) && i <= e; ++i) {
              if (OB_FAIL(rw_.at(i, cur_row))) {
                LOG_WARN("get cur row failed", K(ret), K(i));
              } else if (OB_FAIL(Utils::invoke_agg(*agg_func, use_trans, *cur_row))) {
                LOG_WARN("invoke failed", K(use_trans), K(ret));
              }
            }
          } else {
            LOG_DEBUG("restart agg", K(last_valid_frame), K(new_frame));
            agg_func->reset_for_restart();
            for (int64_t i = new_frame.head_; OB_SUCC(ret) && i <= new_frame.tail_; ++i) {
              if (OB_FAIL(rw_.at(i, cur_row))) {
                LOG_WARN("get cur row failed", K(ret), K(i));
              } else if (OB_FAIL(agg_func->trans(*cur_row))) {
                LOG_WARN("trans failed", K(ret));
              }
            }
          }
        } else {
          LOG_DEBUG("use last value");
          // reuse last result, invoke final directly...
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(agg_func->final(val))) {
            LOG_WARN("final failed", K(ret));
          } else {
            last_valid_frame = new_frame;
            LOG_DEBUG("finish compute", K(row_idx), K(last_valid_frame), K(val));
          }
        }
      } else {
        if (!Utils::same_frame(last_valid_frame, new_frame)) {
          non_agg_func->reset_for_restart();
        }
        if (OB_FAIL(non_agg_func->eval(assist_reader, row_idx, *row, new_frame, val))) {
          LOG_WARN("eval failed", K(ret));
        } else {
          last_valid_frame = new_frame;
        }
      }
    } else {
      // set null for invalid frame
      val.set_null();
      // special case
      if (T_FUN_COUNT == func_ctx->func_info_->func_type_) {
        val.set_int(0);
      }
    }
  }
  return ret;
}

int ObWindowFunction::ObWindowFunctionCtx::get_next_row(const ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (all_first_) {
    DLIST_FOREACH_X(func, func_list_, OB_SUCC(ret))
    {
      if (OB_FAIL(func->init_all_first())) {
        LOG_WARN("init all first failed", K(ret));
      }
    }
  }

  // NULL next_part_first_row_ means iter_end
  if (OB_SUCC(ret) && (rescan_ || all_first_)) {
    next_part_first_row_ = NULL;
    last_output_row_ = -1;
    ret = input_first_row();
    rescan_ = false;
    all_first_ = false;
  }

  while (OB_SUCC(ret)) {
    if (NULL == next_part_first_row_ && rw_.count() - 1 == last_output_row_) {
      LOG_DEBUG("iter end", K(rw_.count() - 1), K(last_output_row_));
      ret = OB_ITER_END;
    } else if (rw_.count() - 1 == last_output_row_) {
      LOG_DEBUG("compute", K(rw_.count() - 1), K(last_output_row_));
      // load && compute
      // new partition(max)

      const uint64_t tenant_id = exec_ctx_.get_my_session()->get_effective_tenant_id();
      // free rows
      ret = rw_.reset(tenant_id);
      last_output_row_ = -1;

      // free rows of last
      DLIST_FOREACH(func, func_list_)
      {
        ret = func->func_ctx_->rw_.reset(tenant_id);
      }

      BaseFunc* func = NULL;
      bool to_next = false;
      const static int64_t CHECK_STATUS_INTERVAL = 10000;
      int64_t check_times = 0;
      while (OB_SUCC(ret)) {
        if (to_next) {
          func = func->get_next();
          LOG_DEBUG("switch to next function", K(*func->func_ctx_));
        } else {
          // restart from first when can't switch to next
          func = func_list_.get_first();
          LOG_DEBUG("restart from first function", K(*func->func_ctx_));
        }
        FuncCtx* func_ctx = func->func_ctx_;
        func_ctx->part_first_row_ = func_ctx->rw_.count();
        func_ctx->part_iter_end_ = NULL == next_part_first_row_;
        if (!to_next) {
          if (OB_FAIL(rw_.add_row(*next_part_first_row_))) {
            LOG_WARN("add row failed", K(ret));
          }
        } else {
          // to_next means that current func(after switch) account a new partition already,
          // no need to detect rows from child_op
          func_ctx->part_iter_end_ = true;
        }

        // input all rows of partition
        while (OB_SUCC(ret) && !func_ctx->part_iter_end_) {
          if (OB_FAIL(input_one_row(func_ctx))) {
            LOG_WARN("input failed", K(ret));
          }
        }

        // compute cell for this func(clone prev cell together)
        if (OB_SUCC(ret)) {
          // reset func before compute
          func->reset_for_restart();
          const ObNewRow* func_row = NULL;
          ObNewRow row;
          ObObj cells[OB_MAX_WINDOW_FUNCTION_NUM];
          ObObj& res_cell = cells[0];
          row.cells_ = cells;
          FuncCtx* prev_func_ctx = NULL;
          RowReader assist_reader(rw_);
          for (int64_t i = func_ctx->part_first_row_; i < rw_.count() && OB_SUCC(ret); ++i) {
            // we should check status interval since this loop will occupy cpu!
            if (0 == ++check_times % CHECK_STATUS_INTERVAL) {
              if (OB_FAIL(exec_ctx_.check_status())) {
                break;
              }
            }
            if (OB_FAIL(compute(assist_reader, func, i, res_cell))) {
              LOG_WARN("compute failed", K(ret));
            } else {
              row.count_ = 1;
              if (func->get_prev() != func_list_.get_header()) {
                prev_func_ctx = func->get_prev()->func_ctx_;
                if (OB_FAIL(prev_func_ctx->rw_.at(i, func_row))) {
                  LOG_WARN("failed to get row", K(ret), K(i));
                } else {
                  row.count_ += func_row->count_;
                }
                // clone cell from prev_func
                for (int64_t j = 0; OB_SUCC(ret) && j < row.get_count() - 1; ++j) {
                  row.cells_[j + 1] = func_row->get_cell(j);
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(func_ctx->rw_.add_row(row))) {
                  LOG_WARN("add row failed", K(ret));
                }
              }
            }
          }
          // free prev
          if (OB_SUCC(ret) && prev_func_ctx != NULL) {
            prev_func_ctx->rw_.reset_buf(tenant_id);
            LOG_DEBUG("reset pre wf_cell", KPC(func_ctx), KPC(prev_func_ctx));
            // instead of using a new variable
            // we reset rows buf only because row count info is needed after
          }
        }

        // check next func
        if (OB_SUCC(ret)) {
          if (func->get_next() == func_list_.get_header()) {
            LOG_DEBUG("compute end", K(*func->func_ctx_), K(last_output_row_));
            // output
            break;
          }
          to_next = NULL == next_part_first_row_;
          if (!to_next) {
            bool same_part = false;
            const ObNewRow* a_row = NULL;
            if (OB_FAIL(rw_.at(func_ctx->part_first_row_, a_row))) {
              LOG_WARN("get part first row failed", K(ret), K(func_ctx->part_first_row_));
            } else if (OB_FAIL(Utils::check_same_partition(func->get_next()->func_ctx_->func_info_->partition_cols_,
                           *a_row,
                           *next_part_first_row_,
                           same_part))) {
              LOG_WARN("check same partition failed", K(ret));
            } else {
              to_next = !same_part;
            }
          }
        }
        LOG_DEBUG("finish restart from first function", K(*func->func_ctx_), K(to_next), K(rw_.count()));
      }
    } else {
      if (op_->is_parallel_ && !finish_parallel_ && OB_FAIL(parallel_winbuf_process())) {
        LOG_WARN("fail to parallel process window function buffer", K(ret));
      } else {
        FuncCtx* func_ctx = func_list_.get_last()->func_ctx_;
        LOG_DEBUG("output", K(last_output_row_), KPC(func_ctx));
        // output
        ObNewRow& cur_row = get_cur_row();
        bool will_break = false;
        for (int64_t i = ++last_output_row_; i < rw_.count() && OB_SUCC(ret); ++i) {
          const ObNewRow* child_row = NULL;
          if (OB_FAIL(rw_.at(i, child_row))) {
            LOG_WARN("get row failed", K(ret), K(i));
          } else {
            for (int64_t j = 0; j < child_row->get_count() && OB_SUCC(ret); ++j) {
              cur_row.cells_[j] = child_row->get_cell(j);
            }
            const ObNewRow* res_row = NULL;
            if (op_->is_parallel_ && OB_FAIL(func_ctx->rw_.at(0, res_row))) {
              LOG_WARN("get row failed", K(ret), K(i));
            } else if (!op_->is_parallel_ && OB_FAIL(func_ctx->rw_.at(i, res_row))) {
              LOG_WARN("get row failed", K(ret), K(i));
            } else {
              int64_t cell_idx = res_row->count_;
              DLIST_FOREACH_NORET(func, func_list_)
              {
                cur_row.cells_[func->func_ctx_->func_info_->result_index_] = res_row->cells_[--cell_idx];
              }
              row = &cur_row;
              will_break = true;
              break;
            }
          }
        }
        if (OB_SUCC(ret) && will_break) {
          break;
        }
      }
    }
  }

  return ret;
}

// send piece msg and wait whole msg.
int ObWindowFunction::ObWindowFunctionCtx::get_whole_msg(bool is_end, ObWinbufWholeMsg& whole, const ObNewRow* res_row)
{
  int ret = OB_SUCCESS;
  const ObWinbufWholeMsg* temp_whole_msg = NULL;
  ObPxSqcHandler* handler = exec_ctx_.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parallel winbuf only supported in parallel execution mode", K(op_->is_parallel_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "parallel winbuf in non-px mode");
  } else {
    ObPxSQCProxy& proxy = handler->get_sqc_proxy();
    ObWinbufPieceMsg piece;
    piece.op_id_ = op_->get_id();
    piece.thread_id_ = GETTID();
    piece.dfo_id_ = proxy.get_dfo_id();
    piece.is_datum_ = false;
    piece.col_count_ = res_row ? res_row->count_ : 0;
    if (is_end) {
      piece.is_end_ = true;
    } else if (OB_ISNULL(res_row)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("row ptr is null", K(ret));
    } else {
      piece.is_end_ = false;
      int64_t len = res_row->get_deep_copy_size();
      char* buf = NULL;
      int64_t pos = 0;
      if (OB_ISNULL(buf = (char*)exec_ctx_.get_allocator().alloc(len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc row buf", K(ret));
      } else if (OB_FAIL(piece.row_.deep_copy(*res_row, buf, len, pos))) {
        LOG_WARN("fail to deep copy row", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(proxy.get_dh_msg(
              op_->get_id(), piece, temp_whole_msg, exec_ctx_.get_physical_plan_ctx()->get_timeout_timestamp()))) {
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

int ObWindowFunction::ObWindowFunctionCtx::parallel_winbuf_process()
{
  int ret = OB_SUCCESS;
  FuncCtx* func_ctx = func_list_.get_last()->func_ctx_;
  const ObNewRow* res_row = NULL;
  ObWinbufWholeMsg whole;
  if (func_ctx->rw_.count() <= 0) {
    if (OB_FAIL(get_whole_msg(true, whole))) {
      LOG_WARN("fail to get whole msg", K(ret));
    }
  } else if (OB_FAIL(func_ctx->rw_.at(0, res_row))) {
    LOG_WARN("get row failed", K(ret), K(res_row));
  } else if (OB_FAIL(get_whole_msg(false, whole, res_row))) {
    LOG_WARN("fail to get whole msg", K(ret));
  } else if (whole.is_empty_) {
    /*do nothing*/
  } else {
    ObChunkRowStore::Iterator row_store_it;
    ObNewRow* new_row = const_cast<ObNewRow*>(res_row);
    ObNewRow* row = NULL;
    bool is_first = true;
    if (OB_FAIL(whole.row_store_.begin(row_store_it))) {
      LOG_WARN("failed to begin iterator for chunk row store", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(row_store_it.get_next_row(row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else if (is_first) {
          is_first = false;
          for (int i = 0; i < new_row->count_ && OB_SUCC(ret); ++i) {
            new_row->cells_[i] = row->cells_[i];
          }
        } else {
          int64_t index = new_row->count_;
          DLIST_FOREACH(func, func_list_)
          {
            index--;
            switch (func->func_ctx_->func_info_->func_type_) {
              case T_FUN_COUNT:
              case T_FUN_SUM: {
                ObObj res;
                if (OB_FAIL(ObExprAdd::calc_for_agg(res, new_row->cells_[index], row->cells_[index], expr_ctx_, -1))) {
                  LOG_WARN("add calculate failed", K(ret), K(index));
                } else if (res.need_deep_copy()) {
                  char* copy_data = NULL;
                  int64_t copy_size = res.get_deep_copy_size();
                  int64_t copy_pos = 0;
                  if (OB_ISNULL(copy_data = static_cast<char*>(exec_ctx_.get_allocator().alloc(copy_size)))) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("memory allocate failed", K(ret));
                  } else if (OB_FAIL(new_row->cells_[index].deep_copy(res, copy_data, copy_size, copy_pos))) {
                    LOG_WARN("obj deep copy failed", K(ret));
                  }
                } else {
                  new_row->cells_[index] = res;
                }
                break;
              }
              case T_FUN_MAX: {
                if (ObObjCmpFuncs::compare_oper_nullsafe(new_row->cells_[index],
                        row->cells_[index],
                        new_row->cells_[index].get_collation_type(),
                        CO_LT)) {
                  new_row->cells_[index] = row->cells_[index];
                }
                break;
              }
              case T_FUN_MIN: {
                if (ObObjCmpFuncs::compare_oper_nullsafe(new_row->cells_[index],
                        row->cells_[index],
                        new_row->cells_[index].get_collation_type(),
                        CO_GT)) {
                  new_row->cells_[index] = row->cells_[index];
                }
                break;
              }
              default: {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("func is not supported", K(ret));
                break;
              }
            }
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        for (int i = 0; i < new_row->count_ && OB_SUCC(ret); ++i) {
          OZ(deep_copy_obj(exec_ctx_.get_allocator(), new_row->cells_[i], new_row->cells_[i]));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    finish_parallel_ = true;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObWindowFunction)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, func_infos_.count()))) {
    LOG_WARN("failed to encode func info count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < func_infos_.count(); i++) {
    if (OB_FAIL(func_infos_.at(i).serialize(buf, buf_len, pos))) {
      LOG_WARN("falied to serialize func info", K(i), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObSingleChildPhyOperator::serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize child operator failed", K(ret));
    }
  }
  OB_UNIS_ENCODE(is_parallel_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObWindowFunction)
{
  int64_t len = 0;

  OB_UNIS_ADD_LEN(func_infos_.count());
  for (int64_t i = 0; i < func_infos_.count(); i++) {
    OB_UNIS_ADD_LEN(func_infos_.at(i));
  }
  len += ObSingleChildPhyOperator::get_serialize_size();
  OB_UNIS_ADD_LEN(is_parallel_);
  return len;
}

OB_DEF_DESERIALIZE(ObWindowFunction)
{
  int ret = OB_SUCCESS;

  int64_t func_cnt = 0;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &func_cnt))) {
    LOG_WARN("decode func count fail", K(ret));
  } else {
    ret = func_infos_.init(func_cnt);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < func_cnt; i++) {
    FuncInfo func_info;
    func_info.my_phy_plan_ = my_phy_plan_;
    if (OB_ISNULL(my_phy_plan_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("my phy plan is null", K(ret));
    } else if (OB_FAIL(func_info.set_allocator(&my_phy_plan_->get_allocator()))) {
      LOG_WARN("failed to set allocator.", K(ret));
    } else if (OB_FAIL(func_info.deserialize(buf, data_len, pos))) {
      LOG_WARN("falied to deserialize func info", K(ret));
    } else {
      ret = func_infos_.push_back(func_info);
    }
  }
  if (OB_SUCC(ret)) {
    ret = ObSingleChildPhyOperator::deserialize(buf, data_len, pos);
  }
  OB_UNIS_DECODE(is_parallel_);
  return ret;
}

int64_t ObWindowFunction::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(func_infos), K_(is_parallel));
  return pos;
}

int ObWindowFunction::ObWindowFunctionCtx::get_pos(RowReader& assist_reader, FuncCtx* func_ctx, const int64_t row_idx,
    const ObNewRow& row, const bool is_rows, const bool is_upper, const bool is_preceding, const bool is_unbounded,
    const bool is_nmb_literal, ObSqlExpression* sql_expr, ObSqlExpression** sql_exprs, int64_t& pos, bool& got_null_val)
{
  LOG_DEBUG("dump",
      K(is_rows),
      K(is_upper),
      K(is_preceding),
      K(is_unbounded),
      K(is_nmb_literal),
      K(row_idx),
      KPC(sql_expr),
      "part first row",
      func_ctx->part_first_row_,
      "part end row",
      rw_.count() - 1,
      "order by cnt",
      func_ctx->func_info_->sort_cols_.count());
  int ret = OB_SUCCESS;
  pos = -1;
  got_null_val = false;
  if (NULL == sql_expr && is_unbounded) {
    // no care rows or range
    pos = is_preceding ? func_ctx->part_first_row_ : rw_.count() - 1;
  } else if (NULL == sql_expr && !is_unbounded) {
    // current row
    if (is_rows) {
      pos = row_idx;
    } else {
      // range
      // for current row, it's no sense for is_preceding
      // we should jump to detect step by step(for case that the sort columns has very small ndv)
      // @TODO: mark detected pos to use for next row

      // pos = row_idx;
      // const int64_t cell_idx = func_ctx->func_info_->sort_cols_.at(0).index_;
      // while (OB_SUCC(ret)) {
      //  if ((is_upper ? --pos < func_ctx->part_first_row_ : ++pos > rw_.count() - 1 ) ||
      //      rw_.at(pos)->get_cell(cell_idx) != rw_.at(row_idx)->get_cell(cell_idx)) {
      //    is_upper ? ++pos : --pos;
      //    break;
      //  }
      //  LOG_DEBUG("dump find pos", K(pos), K(rw_.at(pos)->get_cell(cell_idx)),
      //  K(rw_.at(row_idx)->get_cell(cell_idx)));
      //}

      // Exponential detection
      pos = row_idx;
      int step = 1;
      int64_t cmp_times = 0;
      while (OB_SUCC(ret)) {
        cmp_times++;
        bool match = false;
        const bool overflow = is_upper ? (pos -= step) < func_ctx->part_first_row_ : (pos += step) > rw_.count() - 1;
        if (overflow) {
          match = true;
        } else {
          const ObNewRow* tmp_row = NULL;
          if (OB_FAIL(assist_reader.at(pos, tmp_row))) {
            LOG_WARN("failed to get row", K(pos), K(ret));
          } else {
            for (int64_t i = 0; i < func_ctx->func_info_->sort_cols_.count(); ++i) {
              const int64_t cell_idx = func_ctx->func_info_->sort_cols_.at(i).index_;
              if (tmp_row->get_cell(cell_idx) != row.get_cell(cell_idx)) {
                match = true;
                break;
              }
            }
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
      }
    }
  } else {
    // range or rows with expr
    ObObj val;
    ObObj zero((int64_t)0);
    if (OB_FAIL(sql_expr->calc(expr_ctx_, row, val))) {
      LOG_WARN("calc failed", K(ret));
    } else if (is_nmb_literal) {
      if (val.is_null()) {
        got_null_val = true;
      } else if (share::is_oracle_mode()) {
        int64_t interval = 0;
        if (OB_FAIL(ObWindowFunction::get_param_int_value(expr_ctx_, val, interval))) {
          LOG_WARN("fail to get param int value", K(ret));
        } else if (interval < 0) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("invalid argument", K(ret), K(val));
        }
      } else {
        if (!val.is_numeric_type()) {
          ret = OB_INVALID_NUMERIC;
          LOG_WARN("interval is not numberic", K(ret), K(val));
        } else if (val.get_type() == ObBitType) {
          /*do nothing*/
        } else if (val < zero) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("invalid argument", K(ret), K(val));
        } else { /*do nothing.*/
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_rows) {
        int64_t interval = 0;
        if (OB_FAIL(ObExprUtil::get_trunc_int64(val, expr_ctx_, interval))) {
          LOG_WARN("get_trunc_int64 failed and ignored", K(ret));
        } else {
          pos = is_preceding ? row_idx - interval : row_idx + interval;
        }
        // order by constant or no order by, at this time range n is the entire partition
      } else if (func_ctx->func_info_->sort_cols_.count() == 0) {
        pos = is_preceding ? func_ctx->part_first_row_ : rw_.count() - 1;
      } else {
        // range
        /* This place is a bit detour, for example, for: range between x preceding and y following
         Semantically all lines between (v +/- a) <= x <= (v +/- b)
         +/- is determined by the direction of preceding and sequence, same or -, exclusive or +
         In terms of coding, we need to find the sum of rows that are greater than or equal to
         (v +/- a) minimum value
         Rows less than or equal to the maximum value of (v +/- b)
        */
        const bool is_asc = func_ctx->func_info_->sort_cols_.at(0).is_ascending();
        const int64_t cell_idx = func_ctx->func_info_->sort_cols_.at(0).index_;

        const static int L = 1;
        const static int LE = 1 << 1;
        const static int G = 1 << 2;
        const static int GE = 1 << 3;
        const static int ROLL = L | G;

        int cmp_mode = !(is_preceding ^ is_asc) ? L : G;
        if (is_preceding ^ is_upper) {
          cmp_mode = cmp_mode << 1;
        }
        pos = row_idx;
        int step = cmp_mode & ROLL ? 1 : 0;
        int64_t cmp_times = 0;
        ObObj cmp_val;
        ObCompareCtx cmp_ctx;
        ObCompareCtx* cmp_ctx_ptr = NULL;
        ObCastCtx cast_ctx;
        ObCastCtx* cast_ctx_ptr = NULL;

        ObSqlExpression* cmp_val_expr = (is_preceding ^ is_asc) ? *sql_exprs : *(sql_exprs + 1);
        if (OB_ISNULL(cmp_val_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(cmp_val_expr));
        } else if (OB_FAIL(cmp_val_expr->calc(expr_ctx_, row, cmp_val))) {
          LOG_WARN("calc compare value failed", K(ret));
        }
        while (OB_SUCC(ret)) {
          cmp_times++;
          ObObj cur_val;
          bool match = false;
          const ObNewRow* a_row = NULL;
          const bool overflow =
              is_preceding ? (pos -= step) < func_ctx->part_first_row_ : (pos += step) > rw_.count() - 1;
          if (overflow) {
            match = true;
          } else if (OB_FAIL(assist_reader.at(pos, a_row))) {
            LOG_WARN("failed to get row", K(ret), K(pos));
          } else if (FALSE_IT(cur_val = a_row->get_cell(cell_idx))) {
            // will not reach here
          } else {
            if (NULL == cmp_ctx_ptr || NULL == cast_ctx_ptr) {
              ObSQLSessionInfo* my_session = exec_ctx_.get_my_session();
              ObCastMode cast_mode = CM_NONE;
              int64_t tz_offset = INVALID_TZ_OFF;
              ObObjType cmp_type = ObMaxType;
              if (OB_FAIL(ObSQLUtils::get_default_cast_mode(op_->my_phy_plan_, my_session, cast_mode))) {
                LOG_WARN("failed to get cast_mode", K(ret));
              } else if (OB_FAIL(get_tz_offset(TZ_INFO(my_session), tz_offset))) {
                LOG_WARN("get tz offset failed", K(ret));
              } else if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(
                             cmp_type, cur_val.get_type(), cmp_val.get_type()))) {
                LOG_WARN("fail to get cmp_type", K(ret));
              } else if (ObMaxType == cmp_type) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("invalid cmp type", K(ret));
              } else {
                const bool null_safe = true;
                cmp_ctx_ptr = new (&cmp_ctx)
                    ObCompareCtx(cmp_type, common::CS_TYPE_INVALID, null_safe, tz_offset, default_null_pos());
                const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(my_session);
                cast_ctx_ptr =
                    new (&cast_ctx) ObCastCtx(&local_allocator_, &dtc_params, cast_mode, common::CS_TYPE_INVALID);
              }
            }
            if (OB_SUCC(ret)) {
              ObObj result;
              ObCmpOp cmp_op = CO_MAX;
              if (cmp_mode & L) {
                cmp_op = CO_LT;
              } else if (cmp_mode & LE) {
                cmp_op = CO_LE;
              } else if (cmp_mode & G) {
                cmp_op = CO_GT;
              } else if (cmp_mode & GE) {
                cmp_op = CO_GE;
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid cmp mode", K(ret), K(cmp_op));
              }
              if (OB_SUCC(ret)) {
                ObObj result;
                bool is_true = false;
                if (OB_FAIL(ObRelationalExprOperator::compare(result, cur_val, cmp_val, cmp_ctx, cast_ctx, cmp_op))) {
                  LOG_WARN("failed to compare expr", K(ret), K(cur_val), K(cmp_val), K(cmp_op));
                } else if (OB_FAIL(ObObjEvaluator::is_true(result, is_true))) {
                  LOG_WARN("failed to call is true", K(ret));
                } else if (is_true) {
                  match = is_true;
                }
                LOG_DEBUG("cmp result",
                    K(pos),
                    K(cmp_times),
                    K(cur_val),
                    K(cmp_val),
                    K(cmp_op),
                    K(cmp_ctx.cmp_type_),
                    K(result),
                    K(is_true));
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (match) {
              if (step <= 1) {
                if (cmp_mode & ROLL) {
                  is_preceding ? pos += step : pos -= step;
                }
                break;
              } else {
                is_preceding ? pos += step : pos -= step;
                step = 1;
              }
            } else {
              step = 0 == step ? 1 : (2 * step);
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObWindowFunction::ObWindowFunctionCtx::init()
{
  int ret = OB_SUCCESS;

  finish_parallel_ = false;
  local_allocator_.set_tenant_id(this->exec_ctx_.get_my_session()->get_effective_tenant_id());
  local_allocator_.set_label(ObModIds::OB_SQL_WINDOW_LOCAL);
  local_allocator_.set_ctx_id(ObCtxIds::WORK_AREA);

  FuncAllocer func_alloc;
  func_alloc.local_allocator_ = &local_allocator_;
  for (int64_t i = 0; i < op_->func_infos_.count() && OB_SUCC(ret); ++i) {
    const FuncInfo& func_info = op_->func_infos_.at(i);
    // alloc func_ctx
    void* tmp_ptr = local_allocator_.alloc(sizeof(FuncCtx));
    FuncCtx* func_ctx = NULL;
    if (NULL == tmp_ptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      func_ctx = new (tmp_ptr) FuncCtx();
      func_ctx->func_info_ = const_cast<FuncInfo*>(&func_info);
      func_ctx->w_ctx_ = this;
      const uint64_t tenant_id = exec_ctx_.get_my_session()->get_effective_tenant_id();
      ret = func_ctx->rw_.reset(tenant_id);
    }
    // alloc func
    if (OB_SUCC(ret)) {
      const ObItemType item_type = func_info.func_type_;
      BaseFunc* func = NULL;
      switch (item_type) {
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
        case T_FUN_WM_CONCAT: {
          ret = func_alloc.alloc<AggFunc>(func);
          break;
        }
        case T_WIN_FUN_RANK:
        case T_WIN_FUN_DENSE_RANK:
        case T_WIN_FUN_PERCENT_RANK: {
          ret = func_alloc.alloc<NonAggFuncRankLike>(func);
          break;
        }
        case T_WIN_FUN_CUME_DIST: {
          ret = func_alloc.alloc<NonAggFuncCumeDist>(func);
          break;
        }
        case T_WIN_FUN_ROW_NUMBER: {
          ret = func_alloc.alloc<NonAggFuncRowNumber>(func);
          break;
        }
        // first_value && last_value has been converted to nth_value when resolving
        // case T_WIN_FUN_FIRST_VALUE:
        // case T_WIN_FUN_LAST_VALUE:
        case T_WIN_FUN_NTH_VALUE: {
          ret = func_alloc.alloc<NonAggFuncNthValue>(func);
          break;
        }
        case T_WIN_FUN_LEAD:
        case T_WIN_FUN_LAG: {
          ret = func_alloc.alloc<NonAggFuncLeadOrLag>(func);
          break;
        }
        case T_WIN_FUN_NTILE: {
          ret = func_alloc.alloc<NonAggFuncNtile>(func);
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported function", K(item_type), K(ret));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        func->func_ctx_ = func_ctx;
        if (!func_list_.add_last(func)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("add func failed", K(ret));
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
