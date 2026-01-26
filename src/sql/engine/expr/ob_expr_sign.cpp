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
#include "ob_expr_sign.h"

#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::common::number;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSign::ObExprSign(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_OP_SIGN, N_SIGN, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSign::~ObExprSign()
{
}

int ObExprSign::calc_result_type1(ObExprResType &type,
                                  ObExprResType &text,
                                  common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  // keep enumset as origin type
  if (lib::is_oracle_mode()) {
    type.set_number();
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_scale());
    type.set_precision(
        ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_precision());
  } else {
    type.set_int();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    if (ob_is_numeric_type(text.get_type())) {
      text.set_calc_type(text.get_type());
    } else {
      if (is_oracle_mode()) {
        text.set_calc_type(ObNumberType);
      } else {
        text.set_calc_type(ObDoubleType);
      }
    }
    ObExprOperator::calc_result_flag1(type, text);
  }
  return ret;
}

int ObExprSign::calc(ObObj &result, double val)
{
  if (0 == val) {
    result.set_int(0);
  } else {
    result.set_int(val < 0 ? -1 : 1);
  }
  return OB_SUCCESS;
}

int calc_sign_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg_datum = NULL;
  const ObObjType &arg_type = expr.args_[0]->datum_meta_.type_;
  const ObCollationType &arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  const ObObjType &res_type = expr.datum_meta_.type_;
  const ObObjTypeClass &arg_tc = ob_obj_type_class(arg_type);
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg_datum->is_null()) {
    if (is_oracle_mode()) {
      if (cast_supported(arg_type, arg_cs_type, ObNumberType, CS_TYPE_BINARY)) {
        res_datum.set_null();
      } else {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("invalid type to Number", K(arg_type));
      }
    } else {
      res_datum.set_null();
    }
  } else {
    int64_t res_int = 0;
    switch (arg_tc) {
      case ObIntTC: {
        int64_t v = arg_datum->get_int();
        res_int = v < 0 ? -1 : (0 == v ? 0 : 1);
        break;
      }
      case ObUIntTC: {
        res_int = arg_datum->get_uint64() == 0 ? 0 : 1;
        break;
      }
      case ObNumberTC: {
        number::ObNumber nmb(arg_datum->get_number());
        res_int = nmb.is_negative() ? -1 : (nmb.is_zero() ? 0 : 1);
        break;
      }
      case ObDecimalIntTC: {
        const ObDecimalInt *decint = arg_datum->get_decimal_int();
        const int32_t int_bytes = arg_datum->get_int_bytes();
        res_int = wide::str_helper::is_negative(decint, int_bytes)
                      ? -1
                      : (wide::str_helper::is_zero(decint, int_bytes) ? 0 : 1);
        break;
      }
      case ObFloatTC: {
        float v = arg_datum->get_float();
        if (is_mysql_mode() && 0 == v) {
          res_int = 0;
        } else {
          res_int = v < 0 ? -1 : 1;
        }
        break;
      }
      case ObDoubleTC: {
        double v = arg_datum->get_double();
        if (is_mysql_mode() && 0 == v) {
          res_int = 0;
        } else {
          res_int = v < 0 ? -1 : 1;
        }
        break;
      }
      case ObBitTC: {
        res_int = arg_datum->get_bit() == 0 ? 0 : 1;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected arg_type", K(ret), K(arg_type));
        break;
      }
    }
    if (ObNumberType == res_type) {
      number::ObNumber res_nmb;
      ObNumStackOnceAlloc tmp_alloc;
      if (OB_FAIL(res_nmb.from(res_int, tmp_alloc))) {
        LOG_WARN("get number from int failed", K(ret), K(res_int));
      } else {
        res_datum.set_number(res_nmb);
      }
    } else if (ObIntType == res_type) {
      res_datum.set_int(res_int);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected res_type", K(ret), K(res_type));
    }
  }
  return ret;
}

int ObExprSign::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_sign_expr;
  if (ObIntType == rt_expr.datum_meta_.type_) {
    rt_expr.eval_vector_func_ = eval_sign_vector_int;
  } else if (ObNumberType == rt_expr.datum_meta_.type_) {
    rt_expr.eval_vector_func_ = eval_sign_vector_number;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected result type", K(ret), K(rt_expr.datum_meta_.type_));
  }
  return ret;
}

// Specialized fast path for IntegerFixedVec result type
template <typename ArgVec>
static int vector_sign_fast(ArgVec *arg_vec, IntegerFixedVec *res_vec, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  const int64_t start = bound.start(), end = bound.end(), count = end - start;
  if constexpr (std::is_same_v<ArgVec, IntegerFixedVec>) {
    const int64_t *__restrict arg_array
        = reinterpret_cast<const int64_t *>(arg_vec->get_data() + start * sizeof(int64_t));
    int64_t *__restrict res_array
        = reinterpret_cast<int64_t *>(res_vec->get_data() + start * sizeof(int64_t));
    for (int64_t i = 0; i < count; ++i) {
      int64_t v = arg_array[i];
      res_array[i] = v < 0 ? -1 : (0 == v ? 0 : 1);
    }
  } else if constexpr (std::is_same_v<ArgVec, UIntegerFixedVec>) {
    const uint64_t *__restrict arg_array
        = reinterpret_cast<const uint64_t *>(arg_vec->get_data() + start * sizeof(uint64_t));
    int64_t *__restrict res_array
        = reinterpret_cast<int64_t *>(res_vec->get_data() + start * sizeof(int64_t));
    for (int64_t i = 0; i < count; ++i) {
      res_array[i] = arg_array[i] == 0 ? 0 : 1;
    }
  } else if constexpr (std::is_same_v<ArgVec, FloatFixedVec>) {
    const float *__restrict arg_array
        = reinterpret_cast<const float *>(arg_vec->get_data() + start * sizeof(float));
    int64_t *__restrict res_array
        = reinterpret_cast<int64_t *>(res_vec->get_data() + start * sizeof(int64_t));
    const bool mysql_zero = is_mysql_mode();
    for (int64_t i = 0; i < count; ++i) {
      float v = arg_array[i];
      res_array[i] = (mysql_zero && 0 == v) ? 0 : (v < 0 ? -1 : 1);
    }
  } else if constexpr (std::is_same_v<ArgVec, DoubleFixedVec>) {
    const double *__restrict arg_array
        = reinterpret_cast<const double *>(arg_vec->get_data() + start * sizeof(double));
    int64_t *__restrict res_array
        = reinterpret_cast<int64_t *>(res_vec->get_data() + start * sizeof(int64_t));
    const bool mysql_zero = is_mysql_mode();
    for (int64_t i = 0; i < count; ++i) {
      double v = arg_array[i];
      res_array[i] = (mysql_zero && v == 0) ? 0 : (v < 0 ? -1 : 1);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg_type", K(ret));
  }
  return ret;
}

template <typename ArgVec, typename ResVec, bool IS_INT_RES>
int ObExprSign::vector_sign(const ObExpr &expr,
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
  if constexpr (IS_INT_RES) {
    if (no_skip_no_null && std::is_same_v<ResVec, IntegerFixedVec> && is_fixed_arg) {
      use_fast_path = true;
      if (OB_FAIL(
              vector_sign_fast<ArgVec>(arg_vec, static_cast<IntegerFixedVec *>(res_vec), bound))) {
        LOG_WARN("vector sign fast failed", K(ret));
      } else {
      }
    }
  }
  if (!use_fast_path) {
    const ObObjType &arg_type = expr.args_[0]->datum_meta_.type_;
    const ObObjTypeClass &arg_tc = ob_obj_type_class(arg_type);
    // For ObDecimalIntTC, calculate int_bytes once outside the loop
    int32_t int_bytes = 0;
    if (ObDecimalIntTC == arg_tc) {
      int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(
          expr.args_[0]->datum_meta_.precision_);
    }
    const bool mysql_zero = is_mysql_mode();
    const int64_t start = bound.start(), end = bound.end();
    for (int64_t idx = start; OB_SUCC(ret) && idx < end; ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
      } else {
        int64_t res_int = 0;
        switch (arg_tc) {
          case ObIntTC: {
            int64_t v = arg_vec->get_int(idx);
            res_int = v < 0 ? -1 : (0 == v ? 0 : 1);
            break;
          }
          case ObUIntTC: {
            res_int = arg_vec->get_uint(idx) == 0 ? 0 : 1;
            break;
          }
          case ObFloatTC: {
            float v = arg_vec->get_float(idx);
            res_int = (mysql_zero && 0 == v) ? 0 : (v < 0 ? -1 : 1);
            break;
          }
          case ObDoubleTC: {
            double v = arg_vec->get_double(idx);
            res_int = (mysql_zero && 0 == v) ? 0 : (v < 0 ? -1 : 1);
            break;
          }
          case ObNumberTC: {
            number::ObNumber nmb(arg_vec->get_number(idx));
            res_int = nmb.is_negative() ? -1 : (nmb.is_zero() ? 0 : 1);
            break;
          }
          case ObDecimalIntTC: {
            const ObDecimalInt *decint = arg_vec->get_decimal_int(idx);
            res_int = wide::str_helper::is_negative(decint, int_bytes)
                          ? -1
                          : (wide::str_helper::is_zero(decint, int_bytes) ? 0 : 1);
            break;
          }
          case ObBitTC: {
            res_int = arg_vec->get_bit(idx) == 0 ? 0 : 1;
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected arg_type", K(ret), K(arg_type));
            break;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (IS_INT_RES) {
          res_vec->set_int(idx, res_int);
        } else {
          number::ObNumber res_nmb;
          ObEvalCtx::TempAllocGuard alloc_guard(ctx);
          if (OB_FAIL(res_nmb.from(res_int, alloc_guard.get_allocator()))) {
            LOG_WARN("get number from int failed", K(ret), K(res_int), K(IS_INT_RES));
          }
          res_vec->set_number(idx, res_nmb);
        }
      }
    }
  }

  return ret;
}

int ObExprSign::eval_sign_vector_int(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     const ObBitVector &skip,
                                     const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval sign param", K(ret));
  } else {
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    const ObObjTypeClass arg_tc = ob_obj_type_class(arg_type);
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);

    // MySQL mode: result is int, use IntegerFixedVec for fast path
    if (ObIntTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<IntegerFixedVec, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<IntegerUniVec, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, true>(expr, ctx, skip, bound);
      }
    } else if (ObUIntTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<UIntegerFixedVec, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<UIntegerUniVec, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, true>(expr, ctx, skip, bound);
      }
    } else if (ObFloatTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<FloatFixedVec, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<FloatUniVec, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, true>(expr, ctx, skip, bound);
      }
    } else if (ObDoubleTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<DoubleFixedVec, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<DoubleUniVec, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, true>(expr, ctx, skip, bound);
      }
    } else if (ObNumberTC == arg_tc) {
      if (VEC_DISCRETE == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<NumberDiscVec, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<NumberUniVec, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else if (VEC_CONTINUOUS == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<NumberContVec, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, true>(expr, ctx, skip, bound);
      }
    } else if (ObDecimalIntTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {
        ret = ObExprSign::vector_sign<ObFixedLengthBase, IntegerFixedVec, true>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, true>(expr, ctx, skip, bound);
      }
    } else if (ObBitTC == arg_tc) {
      ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, true>(expr, ctx, skip, bound);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg_type", K(ret), K(arg_tc));
    }
  }

  return ret;
}

int ObExprSign::eval_sign_vector_number(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval sign param", K(ret));
  } else {
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    const ObObjTypeClass arg_tc = ob_obj_type_class(arg_type);
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);

    if (ObIntTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<IntegerFixedVec, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<IntegerUniVec, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, false>(expr, ctx, skip, bound);
      }
    } else if (ObUIntTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<UIntegerFixedVec, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<UIntegerUniVec, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, false>(expr, ctx, skip, bound);
      }
    } else if (ObFloatTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<FloatFixedVec, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<FloatUniVec, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, false>(expr, ctx, skip, bound);
      }
    } else if (ObDoubleTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<DoubleFixedVec, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<DoubleUniVec, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, false>(expr, ctx, skip, bound);
      }
    } else if (ObNumberTC == arg_tc) {
      if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<NumberDiscVec, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<NumberUniVec, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<NumberContVec, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, false>(expr, ctx, skip, bound);
      }
    } else if (ObDecimalIntTC == arg_tc) {
      if (VEC_FIXED == arg_format && VEC_DISCRETE == res_format) {
        ret = ObExprSign::vector_sign<ObFixedLengthBase, NumberDiscVec, false>(expr, ctx, skip, bound);
      } else {
        ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, false>(expr, ctx, skip, bound);
      }
    } else if (ObBitTC == arg_tc) {
      ret = ObExprSign::vector_sign<ObVectorBase, ObVectorBase, false>(expr, ctx, skip, bound);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg_type", K(ret), K(arg_tc));
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
