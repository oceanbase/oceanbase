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

#include "sql/engine/expr/ob_expr_usec_to_time.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprUsecToTime::ObExprUsecToTime(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_USEC_TO_TIME, N_USEC_TO_TIME, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUsecToTime::~ObExprUsecToTime()
{
}

int ObExprUsecToTime::calc_result_type1(ObExprResType &type,
                                        ObExprResType &usec,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(common::ObIntTC == type.get_type_class() && common::ObNullType != usec.get_type())) {
    ret = common::OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME;

  } else {
    type.set_timestamp();
    type.set_scale(common::MAX_SCALE_FOR_TEMPORAL); //和时间值有关，取最大值
    //set calc type
    usec.set_calc_type(ObIntType);
  }
  return ret;
}

int calc_usec_to_time_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *usec_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, usec_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (usec_datum->is_null()) {
    res_datum.set_null();
  } else {
    int64_t value = usec_datum->get_int();
    int32_t offset = 0;
    const ObTimeZoneInfo* tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());
    if (NULL != tz_info && ObTimeConverter::ZERO_DATETIME != value) {
      if (OB_FAIL(tz_info->get_timezone_offset(USEC_TO_SEC(value), offset))) {
        LOG_WARN("failed to get offset between utc and local", K(ret));
      } else {
        value += SEC_TO_USEC(offset);
      }
    }
    if (OB_SUCC(ret)) {
      if (!ObTimeConverter::is_valid_datetime(value)) {
        ret = OB_DATETIME_FUNCTION_OVERFLOW;
        value -= SEC_TO_USEC(offset);
        LOG_WARN("datetime overflow", K(ret), K(value));
      } else {
        value -= SEC_TO_USEC(offset);
        res_datum.set_int(value);
      }
    }
  }
  return ret;
}

#define CHECK_SKIP_NULL(idx) {                 \
  if (skip.at(idx) || eval_flags.at(idx)) {    \
    continue;                                  \
  } else if (arg_vec->is_null(idx)) {          \
    res_vec->set_null(idx);                    \
    continue;                                  \
  }                                            \
}

#define BATCH_CALC(BODY) {                                                        \
  if (OB_LIKELY(no_skip_no_null)) {                                               \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      BODY;                                                                       \
    }                                                                             \
  } else {                                                                        \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      CHECK_SKIP_NULL(idx);                                                       \
      BODY;                                                                       \
    }                                                                             \
  }                                                                               \
}

int ObExprUsecToTime::calc_usec_to_time_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval usec_to_time param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
      ret = do_calc_usec_to_time_vector<DateTimeFixedVec, DateTimeFixedVec>(expr, ctx, skip, bound);
    } else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {
      ret = do_calc_usec_to_time_vector<DateTimeFixedVec, DateTimeUniVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      ret = do_calc_usec_to_time_vector<DateTimeUniVec, DateTimeFixedVec>(expr, ctx, skip, bound);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = do_calc_usec_to_time_vector<DateTimeUniVec, DateTimeUniVec>(expr, ctx, skip, bound);
    } else {
      ret = do_calc_usec_to_time_vector<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprUsecToTime::do_calc_usec_to_time_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  const ObTimeZoneInfo* tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());

  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
    && eval_flags.accumulate_bit_cnt(bound) == 0;
  BATCH_CALC({
    int64_t value = arg_vec->get_int(idx);
    int32_t offset = 0;
    if (NULL != tz_info && ObTimeConverter::ZERO_DATETIME != value) {
      if (OB_FAIL(tz_info->get_timezone_offset(USEC_TO_SEC(value), offset))) {
        LOG_WARN("failed to get offset between utc and local", K(ret));
      } else {
        value += SEC_TO_USEC(offset);
      }
    }
    if (OB_SUCC(ret)) {
      if (!ObTimeConverter::is_valid_datetime(value)) {
        ret = OB_DATETIME_FUNCTION_OVERFLOW;
        value -= SEC_TO_USEC(offset);
        LOG_WARN("datetime overflow", K(ret), K(value));
      } else {
        value -= SEC_TO_USEC(offset);
        res_vec->set_datetime(idx, value);
      }
    }
  });
  return ret;
}

int ObExprUsecToTime::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_usec_to_time_expr;
  rt_expr.eval_vector_func_ = calc_usec_to_time_vector;
  return ret;
}
}
}
