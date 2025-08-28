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

#include "sql/engine/expr/ob_expr_bool.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
namespace oceanbase
{
namespace sql
{

using namespace oceanbase::common;

ObExprBool::ObExprBool(ObIAllocator &alloc)
  : ObLogicalExprOperator(alloc, T_OP_BOOL, N_BOOL, 1, NOT_ROW_DIMENSION) {}

ObExprBool::~ObExprBool() {}

int ObExprBool::calc_result_type1(ObExprResType &type,
                                  ObExprResType &type1,
                                  ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (!lib::is_mysql_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bool expr is only for mysql mode", K(ret));
  } else if (ob_is_numeric_type(type1.get_type()) || ob_is_json(type1.get_type())) {
    type1.set_calc_meta(type1.get_obj_meta());
    type1.set_calc_accuracy(type1.get_accuracy());
    if (ob_is_json(type1.get_type())) {
      type1.set_calc_type(type1.get_type());      
    }
  } else {
    const ObObjType &calc_type = ObDoubleType;
    type1.set_calc_type(calc_type);
    const ObAccuracy &calc_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][calc_type];
    type1.set_calc_accuracy(calc_acc);
  }
  const ObAccuracy &res_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][ObInt32Type];
  type.set_type(ObInt32Type);
  type.set_accuracy(res_acc);
  ObExprOperator::calc_result_flag1(type, type1);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NO_RANGE_CHECK);
  return ret;
}

#define CHECK_IS_TRUE_FUNC_NAME(type)                               \
  int calc_bool_expr_for_##type(const ObExpr &expr, ObEvalCtx &ctx, \
      ObDatum &res_datum)

// ObObjEvaluator::is_true()里面，如果输入obj是null，结果是false
// 但是我们这里需要设置为null
// eg: expr1 and null -> null
// 如果bool expr设置为false，上面结果会变成false
#define EVAL_ARG()                                      \
  int ret = OB_SUCCESS;                                 \
  ObDatum *child_datum = NULL;                          \
  if (OB_FAIL(expr.args_[0]->eval(ctx, child_datum))) { \
    LOG_WARN("eval arg 0 failed", K(ret));              \
  } else if (child_datum->is_null()) {                  \
    res_datum.set_null();                               \
  } else

CHECK_IS_TRUE_FUNC_NAME(integer_type)
{
  EVAL_ARG()
  {
    int32_t res = (0 == child_datum->get_int()) ? 0 : 1;
    res_datum.set_int32(res);
  }
  return ret;
}

CHECK_IS_TRUE_FUNC_NAME(float_type)
{
  EVAL_ARG()
  {
    // 不考虑浮点数和0比较的问题, see ObObj::is_zero()
    int32_t res = (0 == child_datum->get_float()) ? 0 : 1;
    res_datum.set_int32(res);
  }
  return ret;
}

CHECK_IS_TRUE_FUNC_NAME(double_type)
{
  EVAL_ARG()
  {
    // 不考虑浮点数和0比较的问题, see ObObj::is_zero()
    int32_t res = (0 == child_datum->get_double()) ? 0 : 1;
    res_datum.set_int32(res);
  }
  return ret;
}

CHECK_IS_TRUE_FUNC_NAME(other_type)
{
  EVAL_ARG()
  {
    if (ob_is_json(expr.args_[0]->datum_meta_.type_)) {
      int cmp_result = 0;
      ObString j_str = child_datum->get_string();
      ObLobLocatorV2 loc(j_str, expr.args_[0]->obj_meta_.has_lob_header());
      if (OB_FAIL(loc.get_inrow_data(j_str))) {
        if (ret != OB_ERR_NULL_VALUE) {
          COMMON_LOG(WARN, "get lob inrow data failed", K(ret));
        }
        cmp_result = 1; // outrow json must not be zero
      } else if (OB_FAIL(ObJsonExprHelper::is_json_zero(j_str, cmp_result))) {
        LOG_WARN("failed: compare json", K(ret));
      } else {
        res_datum.set_int32(cmp_result);
      }
    } else {
      int32_t res = child_datum->get_number().is_zero() ? 0 : 1;
      res_datum.set_int32(res);      
    }

  }
  return ret;
}

CHECK_IS_TRUE_FUNC_NAME(decint_type)
{
  EVAL_ARG()
  {
    bool is_zero = false;
    switch (get_decimalint_type(expr.args_[0]->datum_meta_.precision_))
    {
    case common::DECIMAL_INT_32:
      is_zero = (*reinterpret_cast<const int32_t *>(child_datum->get_decimal_int()) == 0);
      break;
    case common::DECIMAL_INT_64:
      is_zero = (*reinterpret_cast<const int64_t *>(child_datum->get_decimal_int()) == 0);
      break;
    case common::DECIMAL_INT_128:
      is_zero = (*reinterpret_cast<const int128_t *>(child_datum->get_decimal_int()) == 0);
      break;
    case common::DECIMAL_INT_256:
      is_zero = (*reinterpret_cast<const int256_t *>(child_datum->get_decimal_int()) == 0);
      break;
    case common::DECIMAL_INT_512:
      is_zero = (*reinterpret_cast<const int512_t *>(child_datum->get_decimal_int()) == 0);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected precision", K(ret), K(expr.args_[0]->datum_meta_));
    }
    if (OB_FAIL(ret)) {
    } else {
      res_datum.set_int32(!is_zero);
    }
  }
  return ret;
}

template <typename T>
inline int get_bool(const T &val, const uint32_t len, const ObExpr &expr, bool &res)
{
  UNUSED(len);
  UNUSED(expr);
  res = (0 != val); // No need consider the problem of 'float == 0', see ObObj::is_zero()
  return OB_SUCCESS;
}
template <>
inline int get_bool<>(const number::ObCompactNumber &val,
                      const uint32_t len, const ObExpr &expr, bool &res)
{
  UNUSED(len);
  UNUSED(expr);
  res = !val.is_zero();
  return OB_SUCCESS;
}
template <>
inline int get_bool<>(const ObDecimalInt* const& decimal_int,
                      const uint32_t len, const ObExpr &expr, bool &res)
{
  UNUSED(expr);
  const char *data = reinterpret_cast<const char *>(decimal_int);
  bool is_true = false;
  for (int64_t i = 0; i < len; ++i) {
    if (data[i] != 0) {
      is_true = true;
      break;
    }
  }
  res = is_true;
  return OB_SUCCESS;
}
template <>
inline int get_bool<>(const ObString &j_str,
                      const uint32_t len, const ObExpr &expr, bool &res)
{
  int ret = OB_SUCCESS;
  int is_true = false;
  UNUSED(len);
  ObLobLocatorV2 loc(j_str, expr.args_[0]->obj_meta_.has_lob_header());
  ObString tmp_str = j_str;
  if (OB_FAIL(loc.get_inrow_data(tmp_str))) {
    if (ret != OB_ERR_NULL_VALUE) {
      COMMON_LOG(WARN, "get lob inrow data failed", K(ret));
    }
    is_true = 1; // outrow json must not be zero
  } else if (OB_FAIL(ObJsonExprHelper::is_json_zero(tmp_str, is_true))) {
    LOG_WARN("failed: compare json", K(ret));
  } else {
    res = is_true;
  }
  return ret;
}

template <typename ArgVec, typename ResVec, typename DataType>
static int eval_vector_expr_bool(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 const ObBitVector &skip,
                                 const EvalBound &bound,
                                 DataType (ArgVec::*get_data)(int64_t) const)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  const bool isFixedLenRes = std::is_same<ResVec, IntegerFixedVec>::value;
  int64_t *data_ptr = isFixedLenRes ?
                      const_cast<int64_t*>(reinterpret_cast<const int64_t *>(res_vec->get_payload(0))) :
                      nullptr;
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null();
  if (no_skip_no_null) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      bool arg_is_true = false;
      if (OB_FAIL(get_bool((arg_vec->*get_data)(idx),
                            arg_vec->get_length(idx), expr, arg_is_true))) {
        LOG_WARN("failed to check is true", K(ret));
      } else if (isFixedLenRes) {
        data_ptr[idx] = static_cast<int64_t>(arg_is_true);
      } else {
        res_vec->set_int(idx, static_cast<int64_t>(arg_is_true));
      }
    }
    eval_flags.set_all(bound.start(), bound.end());
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
      } else {
        bool arg_is_true = false;
        if (OB_FAIL(get_bool((arg_vec->*get_data)(idx),
                              arg_vec->get_length(idx), expr, arg_is_true))) {
          LOG_WARN("failed to check is true", K(ret));
        } else {
          res_vec->set_int(idx, static_cast<int64_t>(arg_is_true));
        }
      }
      eval_flags.set(idx);
    }
  }
  return ret;
}

template <typename ArgVec, typename DataType>
static inline int def_eval_vector_bool_expr(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObBitVector &skip,
                                          const EvalBound &bound,
                                          DataType (ObVectorBase::*get_data)(int64_t) const)
{
  int ret = OB_SUCCESS;
  VectorFormat res_format = expr.get_format(ctx);
  // The 'res_format' is expected to be Type 'IntegerFixedVec' under the regular circumstances. 
  // So a condition is added to optimize performance accordingly.
  if (VEC_FIXED == res_format) {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    if (VEC_FIXED == arg_format || VEC_DISCRETE == arg_format || VEC_CONTINUOUS == arg_format) {
      ret = eval_vector_expr_bool<ArgVec, IntegerFixedVec, DataType>
                                (expr, ctx, skip, bound, get_data);
    } else if (VEC_UNIFORM == arg_format) {
      ret = eval_vector_expr_bool<ObUniformBase, IntegerFixedVec, DataType>
                                (expr, ctx, skip, bound, get_data);
    } else {
      ret = eval_vector_expr_bool<ObVectorBase, IntegerFixedVec, DataType>
                                (expr, ctx, skip, bound, get_data);
    }
  } else {
    ret = eval_vector_expr_bool<ObVectorBase, ObVectorBase, DataType>
                                (expr, ctx, skip, bound, get_data);
  }
  return ret;
}

int ObExprBool::calc_vector_bool_expr(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval bool param", K(ret));
  } else {
    ObObjType data_type = expr.args_[0]->datum_meta_.type_;
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    switch(data_type) {
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType:
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type:
      case ObBitType: {
        ret = def_eval_vector_bool_expr<IntegerFixedVec, int64_t>
                                      (expr, ctx, skip, bound, &ObVectorBase::get_int);
        break;
      }
      case ObFloatType:
      case ObUFloatType: {
        ret = def_eval_vector_bool_expr<FloatFixedVec, float>
                                      (expr, ctx, skip, bound, &ObVectorBase::get_float);
        break;
      }
      case ObDoubleType:
      case ObUDoubleType: {
        ret = def_eval_vector_bool_expr<DoubleFixedVec, double>
                                      (expr, ctx, skip, bound, &ObVectorBase::get_double);
        break;
      }
      case ObDecimalIntType: {
        ret = def_eval_vector_bool_expr<ObFixedLengthBase, const ObDecimalInt*>
                                      (expr, ctx, skip, bound, &ObVectorBase::get_decimal_int);
        break;
      }
      case ObJsonType: {
        if (VEC_DISCRETE == arg_format) {
          ret = def_eval_vector_bool_expr<JsonDiscVec, ObString>
                                        (expr, ctx, skip, bound, &ObVectorBase::get_string);
        } else {
          ret = def_eval_vector_bool_expr<JsonContVec, ObString>
                                        (expr, ctx, skip, bound, &ObVectorBase::get_string);
        }
        break;
      }
      default:
        if (VEC_DISCRETE == arg_format) {
          ret = def_eval_vector_bool_expr<NumberDiscVec, const number::ObCompactNumber &>
                                        (expr, ctx, skip, bound, &ObVectorBase::get_number);
        } else {
          ret = def_eval_vector_bool_expr<NumberContVec, const number::ObCompactNumber &>
                                        (expr, ctx, skip, bound, &ObVectorBase::get_number);
        }
        break;
    } //switch
  }
  return ret;
}

int ObExprBool::cg_expr(ObExprCGCtx &expr_cg_ctx,
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  // 布尔表达式只会在MySQL模式下出现, Oracle模式下中如果AND表达式两边不是布尔语义的
  // 在语法解析阶段会报错, 所以resolve时,不会在Oracle模式下增加bool expr
  OB_ASSERT(false == lib::is_oracle_mode());
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_) || OB_ISNULL(rt_expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg cnt is invalid or args_ is NULL", K(ret), K(rt_expr));
  } else {
    const ObDatumMeta child_res_meta = rt_expr.args_[0]->datum_meta_;
    switch (child_res_meta.type_) {
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType:
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type:
      case ObBitType: {
          rt_expr.eval_func_ = calc_bool_expr_for_integer_type;
          break;
      }
      case ObFloatType:
      case ObUFloatType:{
          rt_expr.eval_func_ = calc_bool_expr_for_float_type;
          break;
      }
      case ObDoubleType:
      case ObUDoubleType: {
          rt_expr.eval_func_ = calc_bool_expr_for_double_type;
          break;
      }
      case ObJsonType: {
          rt_expr.eval_func_ = calc_bool_expr_for_other_type;
          break;
      }
      case ObDecimalIntType: {
        rt_expr.eval_func_ = calc_bool_expr_for_decint_type;
        break;
      }
      case ObMaxType: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("bool expr got unexpected type", K(ret), K(child_res_meta));
          break;
      }
      default: {
          rt_expr.eval_func_ = calc_bool_expr_for_other_type;
          break;
      }
    }
    rt_expr.eval_vector_func_ = ObExprBool::calc_vector_bool_expr;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

#undef EVAL_ARG