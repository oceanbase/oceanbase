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
* This file contains implementation for json_valid.
*/

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_is_nan.h"
#include <cmath>

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObExprIsNan::ObExprIsNan(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_CK_SYS_IS_NAN, N_IS_NAN,
                         1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprIsNan::~ObExprIsNan()
{
}

int ObExprIsNan::calc_result_type1(ObExprResType& type,
                                   ObExprResType& type1,
                                   common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  if (ob_is_real_type(type1.get_type()) || ob_is_numeric_type(type1.get_type()) || type1.get_type() == ObNullType) {
    type.set_tinyint();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("unexpected input type for is_nan", K(ret), K(type1.get_type()));
  }
  return ret;
}

int ObExprIsNan::eval_is_nan(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, param))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (param->is_null()) {
    expr_datum.set_null();
  } else {
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    bool is_nan = false;

    if (ob_is_float_tc(arg_type)) {
      float val = param->get_float();
      is_nan = std::isnan(val);
    } else if (ob_is_double_tc(arg_type)) {
      double val = param->get_double();
      is_nan = std::isnan(val);
    } else {
      // Integer, Number, DecimalInt types: never NaN
      is_nan = false;
    }

    expr_datum.set_bool(is_nan);
  }
  return ret;
}

template <typename ArgVec>
static int vector_isnan_fast(ArgVec *arg_vec, IntegerFixedVec *res_vec, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  const int64_t start = bound.start(), end = bound.end();
  int64_t *__restrict res_array
      = reinterpret_cast<int64_t *>(res_vec->get_data() + start * sizeof(int64_t));

  if constexpr (std::is_same_v<ArgVec, FloatFixedVec>) {
    // Float type: check each element with isnan
    for (int64_t idx = start; idx < end; ++idx) {
      float val = arg_vec->get_float(idx);
      res_array[idx - start] = std::isnan(val) ? 1 : 0;
    }
  } else if constexpr (std::is_same_v<ArgVec, DoubleFixedVec>) {
    // Double type: check each element with isnan
    for (int64_t idx = start; idx < end; ++idx) {
      double val = arg_vec->get_double(idx);
      res_array[idx - start] = std::isnan(val) ? 1 : 0;
    }
  } else {
    // Integer types: never NaN
    memset(res_array, 0, (end - start) * sizeof(int64_t));
  }

  return ret;
}

template <typename ArgVec, typename ResVec>
static int vector_isnan_impl(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  const bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null();
  const bool is_fixed_arg
      = std::is_same_v<ArgVec, IntegerFixedVec> || std::is_same_v<ArgVec, UIntegerFixedVec>
        || std::is_same_v<ArgVec, FloatFixedVec> || std::is_same_v<ArgVec, DoubleFixedVec>;
  bool use_fast_path = false;

  // Fast path: no skip, no null, fixed vector format
  if (no_skip_no_null && std::is_same_v<ResVec, IntegerFixedVec> && is_fixed_arg) {
    use_fast_path = true;
    if (OB_FAIL(vector_isnan_fast<ArgVec>(arg_vec, static_cast<IntegerFixedVec *>(res_vec), bound))) {
      LOG_WARN("vector isnan fast failed", K(ret));
    }
  }

  // Slow path: handle NULL values and check NaN
  if (!use_fast_path) {
    const int64_t start = bound.start(), end = bound.end();
    for (int64_t idx = start; OB_SUCC(ret) && idx < end; ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
      } else {
        bool is_nan = false;
        if constexpr (std::is_same_v<ArgVec, FloatFixedVec> || std::is_same_v<ArgVec, FloatUniVec>) {
          float val = arg_vec->get_float(idx);
          is_nan = std::isnan(val);
        } else if constexpr (std::is_same_v<ArgVec, DoubleFixedVec> || std::is_same_v<ArgVec, DoubleUniVec>) {
          double val = arg_vec->get_double(idx);
          is_nan = std::isnan(val);
        } else {
          // Integer, Number, DecimalInt types: never NaN
          is_nan = false;
        }
        res_vec->set_bool(idx, is_nan);
      }
    }
  }

  return ret;
}

int ObExprIsNan::eval_is_nan_vector(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    const ObBitVector &skip,
                                    const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to eval vector result args0", K(ret));
  } else {
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    const ObObjTypeClass arg_tc = ob_obj_type_class(arg_type);
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);

    if (ObIntTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<IntegerFixedVec, IntegerFixedVec>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<IntegerUniVec, IntegerFixedVec>(expr, ctx, skip, bound);
      } else {
        ret = vector_isnan_impl<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else if (ObUIntTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<UIntegerFixedVec, IntegerFixedVec>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<UIntegerUniVec, IntegerFixedVec>(expr, ctx, skip, bound);
      } else {
        ret = vector_isnan_impl<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else if (ObFloatTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<FloatFixedVec, IntegerFixedVec>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<FloatUniVec, IntegerFixedVec>(expr, ctx, skip, bound);
      } else {
        ret = vector_isnan_impl<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else if (ObDoubleTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<DoubleFixedVec, IntegerFixedVec>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<DoubleUniVec, IntegerFixedVec>(expr, ctx, skip, bound);
      } else {
        ret = vector_isnan_impl<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else if (ObNumberTC == arg_tc) {
      if (VEC_DISCRETE == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<NumberDiscVec, IntegerFixedVec>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<NumberUniVec, IntegerFixedVec>(expr, ctx, skip, bound);
      } else if (VEC_CONTINUOUS == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<NumberContVec, IntegerFixedVec>(expr, ctx, skip, bound);
      } else {
        ret = vector_isnan_impl<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else if (ObDecimalIntTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
        ret = vector_isnan_impl<ObFixedLengthBase, IntegerFixedVec>(expr, ctx, skip, bound);
      } else {
        ret = vector_isnan_impl<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
      }
    } else if (ObBitTC == arg_tc) {
      ret = vector_isnan_impl<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg_type for is_nan", K(ret), K(arg_tc));
    }
  }

  return ret;
}

int ObExprIsNan::cg_expr(ObExprCGCtx &op_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_nan function should have 1 param", K(ret));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(rt_expr.args_[0]));
  } else {
    rt_expr.eval_func_ = eval_is_nan;
    if (rt_expr.args_[0]->is_batch_result()) {
      rt_expr.eval_vector_func_ = eval_is_nan_vector;
    }
  }
  return ret;
}
