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

#include "sql/engine/expr/ob_expr_abs.h"
#include "share/datum/ob_datum_util.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
using namespace common::number;

namespace sql
{

#define DEF_EVAL_ABS_FUNC(type)                                \
  template <>                                                  \
  int eval_datum_abs<type>(const ObExpr &expr, ObEvalCtx &ctx, \
                           ObDatum &expr_datum)

static int check_expr_and_eval(const ObExpr &expr, ObEvalCtx &ctx,
                               ObDatum *&param_datum, bool &found_null)
{
  int ret = OB_SUCCESS;
  found_null = false;
  if (OB_UNLIKELY(expr.type_ != T_OP_ABS)
      || OB_UNLIKELY(expr.arg_cnt_ != 1) || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("failed to eval", K(ret));
  } else if (param_datum->is_null()) {
    found_null = true;
  } else {
    // do nothing
  }
  return ret;
}

template<ObObjType obj_type>
int eval_datum_abs(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          ObDatum &expr_datum)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(expr_datum);
  return OB_NOT_SUPPORTED;
}

DEF_EVAL_ABS_FUNC(ObNullType)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(expr);
  expr_datum.set_null();
  return ret;
}

DEF_EVAL_ABS_FUNC(ObNumberType)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param_datum, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    number::ObNumber param_nmb(param_datum->get_number());
    number::ObNumber res_num = param_nmb;
    if (param_nmb.is_negative()) {
      res_num = param_nmb.negate();
    }
    expr_datum.set_number(res_num);
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObUNumberType)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param_datum, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    number::ObNumber param_nmb(param_datum->get_number());
    number::ObNumber res_num = param_nmb;
    if (param_nmb.is_negative()) {
      res_num = param_nmb.negate();
    }
    expr_datum.set_number(res_num);
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObFloatType)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param, found_null))) {
    LOG_WARN("check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    float param_float = param->get_float();
    expr_datum.set_float(param_float >= 0.0f ? param_float : -param_float);
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObDoubleType)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_double(param->get_double() >= 0
                          ? param->get_double() : -param->get_double());
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObUDoubleType)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_double(param->get_udouble());
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObIntType)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    int64_t param_int = param->get_int();
    // 只有mysql模式会调到这个函数，如果发现是INT64_MIN，需要报out of range
    if (INT64_MIN == param_int) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("value out of range", K(ret));
    } else {
      expr_datum.set_int(param_int >= 0 ? param_int : -param_int);
    }
  }
  return ret;
}

DEF_EVAL_ABS_FUNC(ObUInt64Type)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_uint(param->get_uint64());
  }
  return ret;
}

#define MAKE_DECIMAL_INT_OPPOSITE(TYPE)            \
  case sizeof(TYPE##_t): {                         \
    res_val.from(-(*(decint->TYPE##_v_)));         \
    break;                                         \
  }

DEF_EVAL_ABS_FUNC(ObDecimalIntType)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  bool found_null = false;
  if (OB_FAIL(check_expr_and_eval(expr, ctx, param_datum, found_null))) {
    LOG_WARN("failed to check expr and eval", K(ret));
  } else if (found_null) {
    expr_datum.set_null();
  } else {
    const ObDecimalInt *decint = param_datum->get_decimal_int();
    const int32_t int_bytes = param_datum->get_int_bytes();
    bool is_neg = wide::is_negative(decint, int_bytes);
    if (is_neg) {
      ObDecimalIntBuilder res_val;
      switch (int_bytes) {
        MAKE_DECIMAL_INT_OPPOSITE(int32)
        MAKE_DECIMAL_INT_OPPOSITE(int64)
        MAKE_DECIMAL_INT_OPPOSITE(int128)
        MAKE_DECIMAL_INT_OPPOSITE(int256)
        MAKE_DECIMAL_INT_OPPOSITE(int512)
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("int_bytes is unexpected", K(ret), K(int_bytes));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        expr_datum.set_decimal_int(res_val.get_decimal_int(), int_bytes);
      }
    } else {
      expr_datum.set_decimal_int(decint, int_bytes);
    }
  }
  return ret;
}

ObExpr::EvalFunc abs_funcs[ObMaxType];

static int check_expr_and_eval_vector(const ObExpr &expr, ObEvalCtx &ctx,
                               const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expr.type_ != T_OP_ABS)
      || OB_UNLIKELY(expr.arg_cnt_ != 1) || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to eval vector", K(ret));
  }
  return ret;
}

// base
template<ObObjType obj_type>
int eval_vector_abs(const ObExpr &expr,
                    ObEvalCtx &ctx,
                    const ObBitVector &skip,
                    const EvalBound &bound)
{
  UNUSED(expr);
  UNUSED(ctx);
  UNUSED(skip);
  UNUSED(bound);
  return OB_NOT_SUPPORTED;
}

#define DEF_EVAL_ABS_VEC_FUNC(type)                                              \
  template <>                                                                    \
  int eval_vector_abs<type>(const ObExpr &expr, ObEvalCtx &ctx,                  \
                            const ObBitVector &skip, const EvalBound &bound)     \


template<VecValueTypeClass vec_tc, typename ArgVec, typename ResVec>
class EvalVectorRowAbsHelper {
public:
  static int inner_eval_abs_row(const ArgVec *arg_vec, ResVec *res_vec, const int64_t &idx)
  {
    UNUSED(arg_vec);
    UNUSED(res_vec);
    UNUSED(idx);
    return OB_NOT_SUPPORTED;
  }

  static int inner_eval_abs_batch(const ArgVec *arg_vec, ResVec *res_vec,
                                  const EvalBound &bound)
  {
    int ret = OB_SUCCESS;
    for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
      ret = inner_eval_abs_row(arg_vec, res_vec, j);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to eval abs batch", K(ret));
    }
    return ret;
  }
};

#define DEF_INNER_EVAL_ABS_VEC_ROW_FUNC(type)            \
template<typename ArgVec, typename ResVec>               \
class EvalVectorRowAbsHelper<type, ArgVec, ResVec> {     \
  public:                                                \
  static int inner_eval_abs_row(const ArgVec *arg_vec,   \
                                ResVec *res_vec,         \
                                const int64_t &idx)


template<VecValueTypeClass vec_tc, typename ArgVec, typename ResVec>
class EvalVectorAbsHelper {
public:
  static int inner_eval_abs_vector(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   const ObBitVector &skip,
                                   const EvalBound &bound)
  {
    int ret = OB_SUCCESS;
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
    ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
    bool no_skip_no_nulls = bound.get_all_rows_active() && !arg_vec->has_null();
    if (no_skip_no_nulls) {
      ret = EvalVectorRowAbsHelper<vec_tc, ArgVec, ResVec>::
          inner_eval_abs_batch(arg_vec, res_vec, bound);
      if (OB_SUCC(ret)) {
      }
    } else {
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
        if (skip.at(j)) {
          continue;
        }
        if (arg_vec->is_null(j)) {
          res_vec->set_null(j);
        } else {
          ret = EvalVectorRowAbsHelper<vec_tc, ArgVec, ResVec>::
              inner_eval_abs_row(arg_vec, res_vec, j);
        }
      }
    }
    return ret;
  }
};

#define DEF_INNER_EVAL_ABS_VEC_FUNC(type)                     \
template<typename ArgVec, typename ResVec>                    \
class EvalVectorAbsHelper<type, ArgVec, ResVec> {             \
  public:                                                     \
  static int inner_eval_abs_vector(const ObExpr &expr,        \
                                    ObEvalCtx &ctx,           \
                                    const ObBitVector &skip,  \
                                    const EvalBound &bound)

#define END_DEF_INNER_EVAL_ABS_FUNC \
  static int inner_eval_abs_batch(const ArgVec *arg_vec, ResVec *res_vec, \
                                  const EvalBound &bound) \
  { \
    int ret = OB_SUCCESS; \
    for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) { \
      ret = inner_eval_abs_row(arg_vec, res_vec, j); \
    } \
    return ret; \
  } \
};

template<VecValueTypeClass vec_tc>
static int dispatch_eval_abs_fixed_len_vector(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              const ObBitVector &skip,
                                              const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  VectorFormat arg_format = expr.args_[0]->get_format(ctx);
  VectorFormat res_format = expr.get_format(ctx);
  if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
    ret = EvalVectorAbsHelper<vec_tc, ObFixedLengthVector<RTCType<vec_tc>, VectorBasicOp<vec_tc>>,
                              ObFixedLengthVector<RTCType<vec_tc>, VectorBasicOp<vec_tc>>>::
                              inner_eval_abs_vector(expr, ctx, skip, bound);
  } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
    ret = EvalVectorAbsHelper<vec_tc, ObUniformVector<false, VectorBasicOp<vec_tc>>,
                              ObFixedLengthVector<RTCType<vec_tc>, VectorBasicOp<vec_tc>>>::
                              inner_eval_abs_vector(expr, ctx, skip, bound);
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {
    ret = EvalVectorAbsHelper<vec_tc, ObUniformVector<true, VectorBasicOp<vec_tc>>,
                              ObFixedLengthVector<RTCType<vec_tc>, VectorBasicOp<vec_tc>>>::
                              inner_eval_abs_vector(expr, ctx, skip, bound);
  } else {
    ret = EvalVectorAbsHelper<vec_tc, ObVectorBase, ObVectorBase>::
                              inner_eval_abs_vector(expr, ctx, skip, bound);
  }
  return ret;
}

template<VecValueTypeClass vec_tc>
static int dispatch_eval_abs_variable_len_vector(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              const ObBitVector &skip,
                                              const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  VectorFormat arg_format = expr.args_[0]->get_format(ctx);
  VectorFormat res_format = expr.get_format(ctx);
  if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
    ret = EvalVectorAbsHelper<vec_tc, ObDiscreteVector<VectorBasicOp<vec_tc>>,
                              ObDiscreteVector<VectorBasicOp<vec_tc>>>::
                              inner_eval_abs_vector(expr, ctx, skip, bound);
  } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
    ret = EvalVectorAbsHelper<vec_tc, ObContinuousVector<VectorBasicOp<vec_tc>>,
                              ObDiscreteVector<VectorBasicOp<vec_tc>>>::
                              inner_eval_abs_vector(expr, ctx, skip, bound);
  } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
    ret = EvalVectorAbsHelper<vec_tc, ObUniformVector<false, VectorBasicOp<vec_tc>>,
                              ObDiscreteVector<VectorBasicOp<vec_tc>>>::
                              inner_eval_abs_vector(expr, ctx, skip, bound);
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_DISCRETE == res_format) {
    ret = EvalVectorAbsHelper<vec_tc, ObUniformVector<true, VectorBasicOp<vec_tc>>,
                              ObDiscreteVector<VectorBasicOp<vec_tc>>>::
                              inner_eval_abs_vector(expr, ctx, skip, bound);
  } else {
    ret = EvalVectorAbsHelper<vec_tc, ObVectorBase, ObVectorBase>::
                              inner_eval_abs_vector(expr, ctx, skip, bound);
  }
  return ret;
}

// ObNullType
DEF_INNER_EVAL_ABS_VEC_FUNC(VEC_TC_NULL)
{
  int ret = OB_SUCCESS;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
    if (skip.at(j)) {
      continue;
    }
    res_vec->set_null(j);
  }
  return ret;
}
END_DEF_INNER_EVAL_ABS_FUNC

DEF_EVAL_ABS_VEC_FUNC(ObNullType)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_expr_and_eval_vector(expr, ctx, skip, bound))) {
    LOG_WARN("check_expr_and_eval_vector failed", K(ret));
  } else {
      VectorFormat res_format = expr.get_format(ctx);
      switch (res_format) {
      case VEC_DISCRETE:
      case VEC_CONTINUOUS:
      case VEC_FIXED: {
        ret = EvalVectorAbsHelper<VEC_TC_NULL, ObVectorBase, ObBitmapNullVectorBase>::
                                        inner_eval_abs_vector(expr, ctx, skip, bound);
        break;
      }
      case VEC_UNIFORM: {
        ret = EvalVectorAbsHelper<VEC_TC_NULL, ObVectorBase, ObUniformFormat<false>>::
                                inner_eval_abs_vector(expr, ctx, skip, bound);
        break;
      }
      case VEC_UNIFORM_CONST: {
        ret = EvalVectorAbsHelper<VEC_TC_NULL, ObVectorBase, ObUniformFormat<true>>::
                                inner_eval_abs_vector(expr, ctx, skip, bound);
        break;
      }
      default: {
        ret = EvalVectorAbsHelper<VEC_TC_NULL, ObVectorBase, ObVectorBase>::
                                inner_eval_abs_vector(expr, ctx, skip, bound);
      }
    }
  }
  return ret;
}

// ObNumberType
DEF_INNER_EVAL_ABS_VEC_ROW_FUNC(VEC_TC_NUMBER)
{
  number::ObNumber param_nmb(arg_vec->get_number(idx));
  if (param_nmb.is_negative()) {
    res_vec->set_number(idx, param_nmb.negate());
  } else {
    res_vec->set_number(idx, param_nmb);
  }
  return OB_SUCCESS;
}
END_DEF_INNER_EVAL_ABS_FUNC

DEF_EVAL_ABS_VEC_FUNC(ObNumberType)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_expr_and_eval_vector(expr, ctx, skip, bound))) {
    LOG_WARN("check_expr_and_eval_vector failed", K(ret));
  } else if (OB_FAIL(dispatch_eval_abs_variable_len_vector<
                     VEC_TC_NUMBER>(expr, ctx, skip, bound))) {
    LOG_WARN("dispatch_eval_abs_variable_len_vector", K(ret));
  }
  return ret;
}

// ObUNumberType
// The "ObUNumberType" and "ObNumberType" are processed using the same logic.
DEF_EVAL_ABS_VEC_FUNC(ObUNumberType)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_expr_and_eval_vector(expr, ctx, skip, bound))) {
    LOG_WARN("check_expr_and_eval_vector failed", K(ret));
  } else if (OB_FAIL(dispatch_eval_abs_variable_len_vector<
                     VEC_TC_NUMBER>(expr, ctx, skip, bound))) {
    LOG_WARN("dispatch_eval_abs_variable_len_vector", K(ret));
  }
  return ret;
}

DEF_EVAL_ABS_VEC_FUNC(ObFloatType)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_expr_and_eval_vector(expr, ctx, skip, bound))) {
    LOG_WARN("check_expr_and_eval_vector failed", K(ret));
  } else if (OB_FAIL(dispatch_eval_abs_fixed_len_vector<
                      VEC_TC_FLOAT>(expr, ctx, skip, bound))) {
    LOG_WARN("dispatch_eval_abs_variable_len_vector", K(ret));
  }
  return ret;
}

// ObDoubleType
DEF_INNER_EVAL_ABS_VEC_ROW_FUNC(VEC_TC_DOUBLE)
{
  double param_double = arg_vec->get_double(idx);
  res_vec->set_double(idx, param_double >= 0 ? param_double : -param_double);
  return OB_SUCCESS;
}
END_DEF_INNER_EVAL_ABS_FUNC

DEF_INNER_EVAL_ABS_VEC_ROW_FUNC(VEC_TC_FLOAT)
{
  float param_float = arg_vec->get_float(idx);
  res_vec->set_float(idx, param_float >= 0 ? param_float : -param_float);
  return OB_SUCCESS;
}
END_DEF_INNER_EVAL_ABS_FUNC

// Specialization for DoubleFixedVec batch processing
template<>
class EvalVectorRowAbsHelper<VEC_TC_DOUBLE, DoubleFixedVec, DoubleFixedVec> {
public:
  static int inner_eval_abs_row(const DoubleFixedVec *arg_vec,
                                            DoubleFixedVec *res_vec,
                                            const int64_t &idx)
  {
    double param_double = arg_vec->get_double(idx);
    res_vec->set_double(idx, param_double >= 0 ? param_double : -param_double);
    return OB_SUCCESS;
  }

  static int inner_eval_abs_batch(const DoubleFixedVec *arg_vec, DoubleFixedVec *res_vec,
                                  const EvalBound &bound)
  {
    const double *__restrict start_arg
        = reinterpret_cast<const double *>(arg_vec->get_data()) + bound.start();
    double *__restrict start_res
        = reinterpret_cast<double *>(res_vec->get_data()) + bound.start();
    uint16_t length = bound.end() - bound.start();
    for (uint16_t i = 0; i < length; i++) {
      start_res[i] = start_arg[i] >= 0.0 ? start_arg[i] : -start_arg[i];
    }
    return OB_SUCCESS;
  }
};

DEF_EVAL_ABS_VEC_FUNC(ObDoubleType)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_expr_and_eval_vector(expr, ctx, skip, bound))) {
    LOG_WARN("check_expr_and_eval_vector failed", K(ret));
  } else if (OB_FAIL(dispatch_eval_abs_fixed_len_vector<
                    VEC_TC_DOUBLE>(expr, ctx, skip, bound))) {
    LOG_WARN("dispatch_eval_abs_variable_len_vector", K(ret));
  }
  return ret;
}

// ObUDoubleType
DEF_EVAL_ABS_VEC_FUNC(ObUDoubleType)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_expr_and_eval_vector(expr, ctx, skip, bound))) {
    LOG_WARN("check_expr_and_eval_vector failed", K(ret));
  } else if (OB_FAIL(dispatch_eval_abs_fixed_len_vector<
                     VEC_TC_DOUBLE>(expr, ctx, skip, bound))) {
    LOG_WARN("dispatch_eval_abs_variable_len_vector", K(ret));
  }
  return ret;
}

// ObIntType
DEF_INNER_EVAL_ABS_VEC_ROW_FUNC(VEC_TC_INTEGER)
{
  int ret = OB_SUCCESS;
  int64_t param_int = arg_vec->get_int(idx);
  // This function is only called in the MySQL mode.
  // If it is found to be INT64_MIN, it should report an "out of range" error.
  if (INT64_MIN == param_int) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_WARN("int value out of range", K(ret));
  } else {
    res_vec->set_int(idx, param_int >= 0 ? param_int : -param_int);
  }
  return ret;
}
END_DEF_INNER_EVAL_ABS_FUNC

// Specialization for IntegerFixedVec batch processing
template<>
class EvalVectorRowAbsHelper<VEC_TC_INTEGER, IntegerFixedVec, IntegerFixedVec> {
public:
  static int inner_eval_abs_row(const IntegerFixedVec *arg_vec,
                                            IntegerFixedVec *res_vec,
                                            const int64_t &idx)
  {
    int ret = OB_SUCCESS;
    int64_t param_int = arg_vec->get_int(idx);
    // This function is only called in the MySQL mode.
    // If it is found to be INT64_MIN, it should report an "out of range" error.
    if (INT64_MIN == param_int) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("int value out of range", K(ret));
    } else {
      res_vec->set_int(idx, param_int >= 0 ? param_int : -param_int);
    }
    return ret;
  }

  static int inner_eval_abs_batch(const IntegerFixedVec *arg_vec, IntegerFixedVec *res_vec,
                                  const EvalBound &bound)
  {
    int ret = OB_SUCCESS;
    const int64_t *__restrict start_arg
        = reinterpret_cast<const int64_t *>(arg_vec->get_data()) + bound.start();
    int64_t *__restrict start_res
        = reinterpret_cast<int64_t *>(res_vec->get_data()) + bound.start();
    int64_t length = bound.end() - bound.start();
    bool has_int64_min = false;
    for (int64_t i = 0; i < length; ++i) {
      if (start_arg[i] == INT64_MIN) {
        has_int64_min = true;
        break;
      }
    }

    if (OB_UNLIKELY(has_int64_min)) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("int value out of range", K(ret));
    } else {
      for (int64_t i = 0; i < length; i++) {
        start_res[i] = start_arg[i] >= 0 ? start_arg[i] : -start_arg[i];
      }
    }
    return ret;
  }
};

DEF_EVAL_ABS_VEC_FUNC(ObIntType)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_expr_and_eval_vector(expr, ctx, skip, bound))) {
    LOG_WARN("check_expr_and_eval_vector failed", K(ret));
  } else if (OB_FAIL(dispatch_eval_abs_fixed_len_vector<
                     VEC_TC_INTEGER>(expr, ctx, skip, bound))) {
    LOG_WARN("dispatch_eval_abs_variable_len_vector", K(ret));
  }
  return ret;
}

// ObUInt64Type
DEF_INNER_EVAL_ABS_VEC_ROW_FUNC(VEC_TC_UINTEGER)
{
  res_vec->set_uint(idx, arg_vec->get_uint64(idx));
  return OB_SUCCESS;
}
END_DEF_INNER_EVAL_ABS_FUNC

DEF_EVAL_ABS_VEC_FUNC(ObUInt64Type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_expr_and_eval_vector(expr, ctx, skip, bound))) {
    LOG_WARN("check_expr_and_eval_vector failed", K(ret));
  } else if (OB_FAIL(dispatch_eval_abs_fixed_len_vector<
                     VEC_TC_UINTEGER>(expr, ctx, skip, bound))) {
    LOG_WARN("dispatch_eval_abs_variable_len_vector", K(ret));
  }
  return ret;
}

// ObDecimalIntType
#define DEF_INNER_EVAL_ABS_DECIMAL_VEC_FUNC(INT_BIT)                                \
DEF_INNER_EVAL_ABS_VEC_ROW_FUNC(VEC_TC_DEC_INT##INT_BIT)                            \
{                                                                                   \
  const ObDecimalInt *decint = arg_vec->get_decimal_int(idx);                       \
  bool is_neg = wide::is_negative(decint, INT_BIT / CHAR_BIT);                      \
  if (is_neg) {                                                                     \
    ObDecimalIntBuilder res_val;                                                    \
    res_val.from(-(*(decint->int##INT_BIT##_v_)));                                  \
    res_vec->set_decimal_int(idx, res_val.get_decimal_int(), INT_BIT / CHAR_BIT);   \
  } else {                                                                          \
    res_vec->set_decimal_int(idx, decint, INT_BIT / CHAR_BIT);                      \
  }                                                                                 \
  return OB_SUCCESS;                                                                \
}                                                                                   \
END_DEF_INNER_EVAL_ABS_FUNC

// Optimized version for 32-bit decimal int: directly use int32_t
DEF_INNER_EVAL_ABS_VEC_ROW_FUNC(VEC_TC_DEC_INT32)
{
  const ObDecimalInt *decint = arg_vec->get_decimal_int(idx);
  int32_t val = *(decint->int32_v_);
  if (val < 0) {
    int32_t abs_val = -val;
    const ObDecimalInt *abs_decint = reinterpret_cast<const ObDecimalInt *>(&abs_val);
    res_vec->set_decimal_int(idx, abs_decint, sizeof(int32_t));
  } else {
    res_vec->set_decimal_int(idx, decint, sizeof(int32_t));
  }
  return OB_SUCCESS;
}
END_DEF_INNER_EVAL_ABS_FUNC

// Optimized version for 64-bit decimal int: directly use int64_t
DEF_INNER_EVAL_ABS_VEC_ROW_FUNC(VEC_TC_DEC_INT64)
{
  const ObDecimalInt *decint = arg_vec->get_decimal_int(idx);
  int64_t val = *(decint->int64_v_);
  if (val < 0) {
    // Fast path: directly set the negated value
    int64_t abs_val = -val;
    const ObDecimalInt *abs_decint = reinterpret_cast<const ObDecimalInt *>(&abs_val);
    res_vec->set_decimal_int(idx, abs_decint, sizeof(int64_t));
  } else {
    res_vec->set_decimal_int(idx, decint, sizeof(int64_t));
  }
  return OB_SUCCESS;
}
END_DEF_INNER_EVAL_ABS_FUNC

// Specialization for DecInt32FixedVec batch processing
template<>
class EvalVectorRowAbsHelper<VEC_TC_DEC_INT32, DecInt32FixedVec, DecInt32FixedVec> {
public:
  static int inner_eval_abs_row(const DecInt32FixedVec *arg_vec,
                                            DecInt32FixedVec *res_vec,
                                            const int64_t &idx)
  {
    const int32_t *data = reinterpret_cast<const int32_t *>(arg_vec->get_data());
    int32_t val = data[idx];
    if (val < 0) {
      // Fast path: directly set the negated value
      int32_t abs_val = -val;
      int32_t *res_data = reinterpret_cast<int32_t *>(res_vec->get_data());
      res_data[idx] = abs_val;
    } else {
      int32_t *res_data = reinterpret_cast<int32_t *>(res_vec->get_data());
      res_data[idx] = val;
    }
    return OB_SUCCESS;
  }

  static int inner_eval_abs_batch(const DecInt32FixedVec *arg_vec, DecInt32FixedVec *res_vec,
                                  const EvalBound &bound)
  {
    const int32_t *__restrict start_arg
        = reinterpret_cast<const int32_t *>(arg_vec->get_data()) + bound.start();
    int32_t *__restrict start_res
        = reinterpret_cast<int32_t *>(res_vec->get_data()) + bound.start();
    int64_t length = bound.end() - bound.start();

    // Fast path: process all elements directly
    for (int64_t i = 0; i < length; i++) {
      int32_t val = start_arg[i];
      start_res[i] = val >= 0 ? val : -val;
    }
    return OB_SUCCESS;
  }
};

// Specialization for DecInt64FixedVec batch processing
template<>
class EvalVectorRowAbsHelper<VEC_TC_DEC_INT64, DecInt64FixedVec, DecInt64FixedVec> {
public:
  static int inner_eval_abs_row(const DecInt64FixedVec *arg_vec,
                                            DecInt64FixedVec *res_vec,
                                            const int64_t &idx)
  {
    const int64_t *data = reinterpret_cast<const int64_t *>(arg_vec->get_data());
    int64_t val = data[idx];
    if (val < 0) {
      // Fast path: directly set the negated value
      int64_t abs_val = -val;
      int64_t *res_data = reinterpret_cast<int64_t *>(res_vec->get_data());
      res_data[idx] = abs_val;
    } else {
      int64_t *res_data = reinterpret_cast<int64_t *>(res_vec->get_data());
      res_data[idx] = val;
    }
    return OB_SUCCESS;
  }

  static int inner_eval_abs_batch(const DecInt64FixedVec *arg_vec, DecInt64FixedVec *res_vec,
                                  const EvalBound &bound)
  {
    const int64_t *__restrict start_arg
        = reinterpret_cast<const int64_t *>(arg_vec->get_data()) + bound.start();
    int64_t *__restrict start_res
        = reinterpret_cast<int64_t *>(res_vec->get_data()) + bound.start();
    int64_t length = bound.end() - bound.start();

    // Fast path: process all elements directly
    // Note: INT64_MIN case is extremely rare in decimal int context, so we optimize for common case
    for (int64_t i = 0; i < length; i++) {
      int64_t val = start_arg[i];
      start_res[i] = val >= 0 ? val : -val;
    }
    return OB_SUCCESS;
  }
};

DEF_INNER_EVAL_ABS_DECIMAL_VEC_FUNC(128)
DEF_INNER_EVAL_ABS_DECIMAL_VEC_FUNC(256)
DEF_INNER_EVAL_ABS_DECIMAL_VEC_FUNC(512)

DEF_EVAL_ABS_VEC_FUNC(ObDecimalIntType)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_expr_and_eval_vector(expr, ctx, skip, bound))) {
    LOG_WARN("check_expr_and_eval_vector failed", K(ret));
  } else {
    int16_t precision = expr.datum_meta_.precision_;
    if (precision <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("dicimal precision <= 0", K(ret), K(precision));
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_32) {
      ret = dispatch_eval_abs_fixed_len_vector<VEC_TC_DEC_INT32>(expr, ctx, skip, bound);
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_64) {
      ret = dispatch_eval_abs_fixed_len_vector<VEC_TC_DEC_INT64>(expr, ctx, skip, bound);
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_128) {
      ret = dispatch_eval_abs_fixed_len_vector<VEC_TC_DEC_INT128>(expr, ctx, skip, bound);
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_256) {
      ret = dispatch_eval_abs_fixed_len_vector<VEC_TC_DEC_INT256>(expr, ctx, skip, bound);
    } else {
      ret = dispatch_eval_abs_fixed_len_vector<VEC_TC_DEC_INT512>(expr, ctx, skip, bound);
    }
  }
  return ret;
}

ObExpr::EvalVectorFunc abs_vec_funcs[ObMaxType];

template<int IDX>
struct AbsFuncIniter
{
  static bool init_array()
  {
    abs_funcs[IDX] = &eval_datum_abs<static_cast<ObObjType>(IDX)>;
    abs_vec_funcs[IDX] = &eval_vector_abs<static_cast<ObObjType>(IDX)>;
    return true;
  }
};

// 特化：ObTinyIntType, ObSmallIntType, ObInt32Type 使用 ObIntType 的求值函数
template<>
struct AbsFuncIniter<ObTinyIntType>
{
  static bool init_array()
  {
    abs_funcs[ObTinyIntType] = &eval_datum_abs<ObIntType>;
    abs_vec_funcs[ObTinyIntType] = &eval_vector_abs<ObIntType>;
    return true;
  }
};

template<>
struct AbsFuncIniter<ObSmallIntType>
{
  static bool init_array()
  {
    abs_funcs[ObSmallIntType] = &eval_datum_abs<ObIntType>;
    abs_vec_funcs[ObSmallIntType] = &eval_vector_abs<ObIntType>;
    return true;
  }
};

template<>
struct AbsFuncIniter<ObInt32Type>
{
  static bool init_array()
  {
    abs_funcs[ObInt32Type] = &eval_datum_abs<ObIntType>;
    abs_vec_funcs[ObInt32Type] = &eval_vector_abs<ObIntType>;
    return true;
  }
};

template<>
struct AbsFuncIniter<ObMediumIntType>
{
  static bool init_array()
  {
    abs_funcs[ObMediumIntType] = &eval_datum_abs<ObIntType>;
    abs_vec_funcs[ObMediumIntType] = &eval_vector_abs<ObIntType>;
    return true;
  }
};

// 特化：ObUTinyIntType, ObUSmallIntType, ObUInt32Type, ObUMediumIntType 使用 ObUInt64Type 的求值函数
template<>
struct AbsFuncIniter<ObUTinyIntType>
{
  static bool init_array()
  {
    abs_funcs[ObUTinyIntType] = &eval_datum_abs<ObUInt64Type>;
    abs_vec_funcs[ObUTinyIntType] = &eval_vector_abs<ObUInt64Type>;
    return true;
  }
};

template<>
struct AbsFuncIniter<ObUSmallIntType>
{
  static bool init_array()
  {
    abs_funcs[ObUSmallIntType] = &eval_datum_abs<ObUInt64Type>;
    abs_vec_funcs[ObUSmallIntType] = &eval_vector_abs<ObUInt64Type>;
    return true;
  }
};

template<>
struct AbsFuncIniter<ObUInt32Type>
{
  static bool init_array()
  {
    abs_funcs[ObUInt32Type] = &eval_datum_abs<ObUInt64Type>;
    abs_vec_funcs[ObUInt32Type] = &eval_vector_abs<ObUInt64Type>;
    return true;
  }
};

template<>
struct AbsFuncIniter<ObUMediumIntType>
{
  static bool init_array()
  {
    abs_funcs[ObUMediumIntType] = &eval_datum_abs<ObUInt64Type>;
    abs_vec_funcs[ObUMediumIntType] = &eval_vector_abs<ObUInt64Type>;
    return true;
  }
};

static bool abs_eval_func_init_ret = ObArrayConstIniter<ObMaxType, AbsFuncIniter>::init();

static_assert(ObMaxType == sizeof(abs_funcs) / sizeof(void *), "unexpected size");

static_assert(ObMaxType == sizeof(abs_vec_funcs) / sizeof(void *), "unexpected size");
REG_SER_FUNC_ARRAY(OB_SFA_SQL_EXPR_ABS_EVAL, abs_funcs, ARRAYSIZEOF(abs_funcs));

REG_SER_FUNC_ARRAY(OB_SFA_SQL_EXPR_ABS_EVAL_VEC, abs_vec_funcs, ARRAYSIZEOF(abs_vec_funcs));

ObExprAbs::ObExprAbs(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_OP_ABS, N_ABS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
      func_(NULL) {}

int ObExprAbs::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprAbs *tmp_other = dynamic_cast<const ObExprAbs *>(&other);
  if (OB_UNLIKELY(NULL == tmp_other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (OB_LIKELY(this != tmp_other)) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(ret));
    } else {
      this->func_ = tmp_other->func_;
    }
  }
  return ret;
}

int ObExprAbs::calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                 ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (NOT_ROW_DIMENSION == row_dimension_) {
    // result type
    ObObjType itype;
    if (OB_SUCC(ObExprResultTypeUtil::get_abs_result_type(itype, type1.get_type()))) {
      if (lib::is_oracle_mode() && ob_is_json(type1.get_type())) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
      } else if (ObMaxType == itype) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
      } else {
        type.set_type(itype);
      }
    }

    // collation
    // 结果不可能为字符类型，无需专门设置collation
    if (lib::is_oracle_mode() && (type1.is_varchar_or_char() || type1.is_number_float())) {
      type.set_precision(PRECISION_UNKNOWN_YET);
      type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
    } else if (lib::is_mysql_mode() && type.is_double() && type1.get_scale() != SCALE_UNKNOWN_YET) {
      type.set_scale(type1.get_scale());
      type.set_precision(static_cast<ObPrecision>(ObMySQLUtil::float_length(type1.get_scale())));
    } else {
      type.set_accuracy(type1.get_accuracy());
    }

    // null flag
    ObExprOperator::calc_result_flag1(type, type1);

    if (OB_SUCC(ret)) {
      // set calc type for param
      ObObjType param_calc_type = calc_param_type(type1.get_type(),
                                                  lib::is_oracle_mode());
      if (OB_UNLIKELY(ObMaxType == param_calc_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param calc type", K(ret), K(type1.get_type()), K(param_calc_type));
      } else {
        type1.set_calc_type(param_calc_type);
        if (type1.get_type() == ObJsonType) {
          type1.set_calc_type(ObDoubleType);
          type.set_type(ObDoubleType);
        }
      }
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  }
  return ret;
}

//tinyint, mediumint, smallint, int32
int ObExprAbs::abs_int(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  res.set_int(param.get_int() >= 0 ? param.get_int() : -param.get_int());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//int64
int ObExprAbs::abs_int64(ObObj &res,
                     const ObObj &param,
                     ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  if (INT64_MIN == param.get_int()) {
    ret = OB_OPERATE_OVERFLOW; //INT64_MIN时，mysql会返回一个out of range的错误
    LOG_WARN("value out of range", K(ret), K(INT64_MIN), K(param));
  } else {
    res.set_int(param.get_int() >= 0LL ? param.get_int() : -param.get_int());
  }
  UNUSED(expr_ctx);
  return ret;
}
//utiniyint, umediumint, usmallint
int ObExprAbs::abs_uint(ObObj &res,
                    const ObObj &param,
                    ObExprCtx &expr_ctx)
{
  res.set_uint64(static_cast<uint64_t>(param.get_uint32()));
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//uint32 uint64
int ObExprAbs::abs_uint32_uint64(ObObj &res,
                             const ObObj &param,
                             ObExprCtx &expr_ctx)
{
  res.set_uint64(param.get_uint64());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//float
int ObExprAbs::abs_float(ObObj &res,
                     const ObObj &param,
                     ObExprCtx &expr_ctx)
{
  res.set_float(param.get_float() >= 0.0f ? param.get_float() : -param.get_float());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
int ObExprAbs::abs_float_double(ObObj &res,
                     const ObObj &param,
                     ObExprCtx &expr_ctx)
{
  res.set_double(static_cast<double>(param.get_float() >= 0.0f ?
                                        param.get_float() :
                                        -param.get_float()));
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//double
int ObExprAbs::abs_double(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  res.set_double(param.get_double() >= 0.0 ? param.get_double() : -param.get_double());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//ufloat
int ObExprAbs::abs_ufloat_udouble(ObObj &res,
                     const ObObj &param,
                     ObExprCtx &expr_ctx)
{
  res.set_udouble(static_cast<double>(param.get_ufloat()));
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//udouble
int ObExprAbs::abs_udouble(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  res.set_udouble(param.get_udouble());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//number
int ObExprAbs::abs_number(ObObj &res,
                     const ObObj &param,
                     ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_number() && !param.is_number_float())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(param), K(ret));
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    LOG_WARN("allocator should not be null");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    number::ObNumber param_nmb = param.get_number();
    number::ObNumber res_nmb;
    if (param_nmb.is_negative()) {
      ret = param_nmb.negate(res_nmb, *expr_ctx.calc_buf_);
    } else {
      ret = res_nmb.from(param_nmb, *expr_ctx.calc_buf_);
    }
    if (OB_SUCC(ret)) {
      res.set_number(res_nmb);
    }
  }
  return ret;
}
//unumber
int ObExprAbs::abs_unumber(ObObj &res,
                      const ObObj &param,
                      ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  TYPE_CHECK(param, ObUNumberType);
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    LOG_WARN("allocator should not be null");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    number::ObNumber param_nmb = param.get_unumber();
    number::ObNumber res_nmb;
    if (OB_FAIL(res_nmb.from(param_nmb, *expr_ctx.calc_buf_))) {
      LOG_WARN("deep copy number failed", K(ret), K(param_nmb));
    } else {
      res.set_unumber(res_nmb);
    }
  }
  return ret;
}

//null
int ObExprAbs::abs_null(ObObj &res,
                      const ObObj &param,
                      ObExprCtx &expr_ctx)
{
  res.set_null();
  UNUSED(param);
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}

//others. (datetime time varchar, etc)
int ObExprAbs::abs_others_double(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  double value = 0.0;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  EXPR_GET_DOUBLE_V2(param, value);
  if (OB_SUCC(ret)) {
    res.set_double(value >= 0.0 ? value : -value);
  }
  return ret;
}

//others for oracle. (datetime time varchar, etc)
int ObExprAbs::abs_others_number(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  number::ObNumber value;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  EXPR_GET_NUMBER_V2(param, value);
  if (OB_SUCC(ret)) {
    res.set_number(value.is_negative() ? value.negate() : value);
  }
  return ret;
}

int ObExprAbs::abs_hexstring(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  double value = 0.0;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  EXPR_GET_DOUBLE_V2(param, value);
  if (OB_SUCC(ret)) {
    //udouble, not double. compatible with mysql.
    res.set_udouble(value >= 0.0 ? value : -value);
  }
  return ret;
}

int ObExprAbs::abs_year(ObObj &res,
                   const ObObj &param,
                   ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0.0;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  EXPR_GET_UINT64_V2(param, value);
  if (OB_SUCC(ret)) {
    res.set_uint64(value);//abs(year) returns uint64. compatible with mysql.
  }
  return ret;
}

//bit
int ObExprAbs::abs_bit(ObObj &res,
                       const ObObj &param,
                       ObExprCtx &expr_ctx)
{
  res.set_uint64(param.get_bit());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}
//enum_set
int ObExprAbs::abs_enum_set(ObObj &res,
                            const ObObj &param,
                            ObExprCtx &expr_ctx)
{
  res.set_uint64(param.get_uint64());
  UNUSED(expr_ctx);
  return OB_SUCCESS;
}

int ObExprAbs::cg_expr(ObExprCGCtx &ctx,
                       const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(T_OP_ABS != rt_expr.type_)
      || OB_ISNULL(rt_expr.args_)
      || OB_UNLIKELY(rt_expr.arg_cnt_ !=  1)
      || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(rt_expr.args_[0]->datum_meta_.type_ >= ObMaxType)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg type for abs", K(ret));
  } else {
    rt_expr.eval_func_ = abs_funcs[rt_expr.args_[0]->datum_meta_.type_];
    rt_expr.eval_vector_func_ = abs_vec_funcs[rt_expr.args_[0]->datum_meta_.type_];
  }
  return ret;
}

ObObjType ObExprAbs::calc_param_type(const ObObjType orig_param_type,
                                     const bool is_oracle_mode)
{
  ObObjType calc_type = ObMaxType;
  if (is_oracle_mode) {
    switch (orig_param_type)
    {
     case ObNullType: {
       calc_type = ObNullType;
        break;
     }
     case ObFloatType: {
       calc_type = ObFloatType;
       break;
     }
     case ObDoubleType: {
       calc_type = ObDoubleType;
       break;
     }
     case ObNumberType:
     case ObTinyIntType:
     case ObSmallIntType:
     case ObInt32Type:
     case ObIntType:
     case ObNumberFloatType:
     case ObTimestampTZType:
     case ObTimestampLTZType:
     case ObTimestampNanoType:
     case ObCharType:
     case ObVarcharType:
     case ObIntervalDSType:
     case ObIntervalYMType:
     case ObNVarchar2Type:
     case ObNCharType:
     case ObURowIDType:
     case ObDecimalIntType: {
       calc_type = ObNumberType;
       break;
     }
     default: {
       // do nothing
       break;
     }
    }
  } else {
    switch (orig_param_type)
    {
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType:
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type: {
      calc_type = orig_param_type;
      break;
    }
    case ObFloatType:
    case ObDoubleType: {
      calc_type = ObDoubleType;
      break;
    }
    case ObUFloatType:
    case ObUDoubleType: {
      calc_type = ObUDoubleType;
      break;
    }
    case ObNumberType: {
      calc_type = ObNumberType;
      break;
    }
    case ObUNumberType: {
      calc_type = ObUNumberType;
      break;
    }
    case ObNullType: {
      calc_type = ObNullType;
      break;
    }
    case ObYearType: {
      calc_type = ObUInt64Type;
      break;
    }
    case ObDateTimeType:
    case ObTimestampType:
    case ObDateType:
    case ObTimeType:
    case ObVarcharType:
    case ObCharType:
    case ObUnknownType:
    case ObHexStringType:
    case ObTextType:
    case ObTinyTextType:
    case ObMediumTextType:
    case ObLongTextType:
    case ObMySQLDateType:
    case ObMySQLDateTimeType: {
      calc_type = ObDoubleType;
      break;
    }
    case ObBitType: {
      calc_type = ObUInt64Type;
      break;
    }
    case ObEnumType:
    case ObSetType:
    case ObJsonType: {
      calc_type = ObDoubleType;
      break;
    }
    case ObDecimalIntType: {
      calc_type = ObDecimalIntType;
      break;
    }
    default: {
      break;
    }
    }
  }
  return calc_type;
}

} // namespace sql
} // namespace oceanbase
