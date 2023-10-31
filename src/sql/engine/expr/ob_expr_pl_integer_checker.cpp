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

#include "ob_expr_pl_integer_checker.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace pl;
namespace sql
{

OB_SERIALIZE_MEMBER(ObExprPLIntegerChecker::ExtraInfo,
                    pl_integer_type_, pl_integer_range_.range_);

void ObExprPLIntegerChecker::ExtraInfo::reset()
{
  pl_integer_type_ = ObPLIntegerType::PL_INTEGER_INVALID;
  pl_integer_range_.reset();
}

int ObExprPLIntegerChecker::ExtraInfo::deep_copy(common::ObIAllocator &allocator,
                                        const ObExprOperatorType type,
                                        ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  ExtraInfo *copied_cursor_info = NULL;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    LOG_WARN("failed to alloc expr extra info", K(ret));
  } else if (OB_ISNULL(copied_cursor_info = static_cast<ExtraInfo *>(copied_info))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else {
    copied_cursor_info->pl_integer_type_ = pl_integer_type_;
    copied_cursor_info->pl_integer_range_ = pl_integer_range_;
  }
  return ret;
}

int ObExprPLIntegerChecker::ExtraInfo::assign(const ExtraInfo &other)
{
  int ret = OB_SUCCESS;
  pl_integer_type_ = other.pl_integer_type_;
  pl_integer_range_ = other.pl_integer_range_;
  return ret;
}

OB_SERIALIZE_MEMBER((ObExprPLIntegerChecker, ObFuncExprOperator),
                    info_.pl_integer_type_, info_.pl_integer_range_.range_);

ObExprPLIntegerChecker::ObExprPLIntegerChecker(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_PL_INTEGER_CHECKER, N_PL_INTEGER_CHECKER, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, INTERNAL_IN_ORACLE_MODE),
      info_(alloc, T_FUN_PL_INTEGER_CHECKER) {}

ObExprPLIntegerChecker::~ObExprPLIntegerChecker() {}

int ObExprPLIntegerChecker::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprPLIntegerChecker *tmp = dynamic_cast<const ObExprPLIntegerChecker *>(&other);
  if (OB_UNLIKELY(OB_ISNULL(tmp))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(other), K(ret));
  } else if (OB_LIKELY(this != tmp)) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(other), K(ret));
    } else {
      OZ (info_.assign(tmp->info_));
    }
  }
  return ret;
}

int ObExprPLIntegerChecker::calc_result_type1(ObExprResType &type,
                                              ObExprResType &type1,
                                              ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  // ExprPLIntegerChecker用来check pl中integer数据类型的结果合法性, 不会改变结果的数据类型
  type.reset();
  if (OB_FAIL(type.assign(type1))) {
    LOG_WARN("fail to assign ObExprResType", K(type1), K(ret));
  }
  return ret;
}

template<typename T>
int ObExprPLIntegerChecker::check_range(const T &obj, const ObObjType type, int64_t range)
{
  int ret = OB_SUCCESS;
#define GET_VALUE_AND_CHECK(type, v_type, func, min, max)       \
  case type: {                                                  \
    const v_type &v = obj.func();                                     \
    if (v < min || v > max) {                                   \
      ret = OB_NUMERIC_OVERFLOW;                                \
      LOG_WARN("value is out of range", K(ret), K(v), K(obj), K(min), K(max));  \
    }                                                           \
    break;                                                      \
  }

#define CHECK_OVERFLOW(min, max)                                                                \
  switch(type) {                                                                      \
    GET_VALUE_AND_CHECK(ObTinyIntType, int64_t, get_tinyint, min, max);                         \
    GET_VALUE_AND_CHECK(ObSmallIntType, int64_t, get_smallint, min, max);                       \
    GET_VALUE_AND_CHECK(ObMediumIntType, int64_t, get_mediumint, min, max);                     \
    GET_VALUE_AND_CHECK(ObInt32Type, int64_t, get_int32, min, max);                             \
    GET_VALUE_AND_CHECK(ObIntType, int64_t, get_int, min, max);                                 \
    GET_VALUE_AND_CHECK(ObUTinyIntType, uint64_t, get_utinyint, (min <= 0 ? 1 : min), max);     \
    GET_VALUE_AND_CHECK(ObUSmallIntType, uint64_t, get_usmallint, (min <= 0 ? 1 : min), max);   \
    GET_VALUE_AND_CHECK(ObUMediumIntType, uint64_t, get_umediumint, (min <= 0 ? 1 : min), max); \
    GET_VALUE_AND_CHECK(ObUInt32Type, uint64_t, get_uint32, (min <= 0 ? 1 : min), max);         \
    GET_VALUE_AND_CHECK(ObUInt64Type, uint64_t, get_uint64, (min <= 0 ? 1 : min), max);         \
    GET_VALUE_AND_CHECK(ObFloatType, float, get_float, min, max);                               \
    GET_VALUE_AND_CHECK(ObDoubleType, double, get_double, min, max);                            \
    GET_VALUE_AND_CHECK(ObUFloatType, float, get_ufloat, (min <= 0 ? 1 : min), max);            \
    GET_VALUE_AND_CHECK(ObUDoubleType, double, get_udouble, (min <= 0 ? 1 : min), max);         \
    GET_VALUE_AND_CHECK(ObNumberType, number::ObNumber, get_number,                             \
                        static_cast<int64_t>(min), static_cast<int64_t>(max));                  \
    GET_VALUE_AND_CHECK(ObUNumberType, number::ObNumber, get_number,                           \
                        static_cast<int64_t>(min), static_cast<int64_t>(max));                  \
    default: {                                                                                  \
    }                                                                                           \
  } // TODO:@xiaofeng.lby, 这里应该需要处理 decimal int 类型，PL 相关
  ObPLIntegerRange pls_range(range);
  if (pls_range.valid()) {
    CHECK_OVERFLOW(pls_range.get_lower(), pls_range.get_upper());
  }
#undef GET_VALUE_AND_CHECK
#undef CHECK_OVERFLOW
  return ret;
}

int ObExprPLIntegerChecker::calc(ObObj &result,
                                 const ObObj &obj,
                                 const pl::ObPLIntegerType &pls_type,
                                 const pl::ObPLIntegerRange &pls_range,
                                 ObIAllocator &calc_buf)
{
  int ret = OB_SUCCESS;
  switch (pls_type) {
    // SimpleInteger的溢出行为有差别, 如果溢出需要圆整下, 其他的直接校验
    case PL_SIMPLE_INTEGER: {
      int64_t v = 0;
      if (obj.is_integer_type()) {
        v = obj.get_int();
      } else if (obj.is_number()) {
        OZ (obj.get_number().extract_valid_int64_with_trunc(v));
      }
      if (OB_SUCC(ret)) {
        if (v < pls_range.lower_ || v > pls_range.upper_) {
          v = static_cast<int32_t>(v);
          if (obj.is_integer_type()) {
            result.set_int(obj.get_type(), v);
          } else if (obj.is_number()) {
            number::ObNumber n;
            OZ (n.from(v, calc_buf));
            OX (result.set_number(obj.get_type(), n));
          }
        } else {
          result = obj;
        }
      }
      break;
    }
    case PL_PLS_INTEGER:
    case PL_BINARY_INTEGER:
    case PL_NATURAL:
    case PL_NATURALN:
    case PL_POSITIVE:
    case PL_POSITIVEN:
    case PL_SIGNTYPE: {
      OZ (check_range(obj, obj.get_type(), pls_range.range_));
      OX (result = obj);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pl integer type", K(pls_type), K(pls_range), K(ret));
    }
  }
  return ret;
}

int ObExprPLIntegerChecker::calc(ObDatum &res_datum,
                                 const ObObjType type,
                                 const ObDatum &param,
                                 const pl::ObPLIntegerType &pls_type,
                                 const pl::ObPLIntegerRange &pls_range,
                                 ObIAllocator &calc_buf)
{
  int ret = OB_SUCCESS;
  switch (pls_type) {
    case PL_SIMPLE_INTEGER: {
      int64_t v = 0;
      if (ob_is_integer_type(type)) {
        v = param.get_int();
      } else if (ObNumberType == type) {
        number::ObNumber nmb(param.get_number());
        OZ (nmb.extract_valid_int64_with_trunc(v));
      }
      if (OB_SUCC(ret)) {
        if (v < pls_range.lower_ || v > pls_range.upper_) {
          v = static_cast<int32_t>(v);
          if (ob_is_integer_type(type)) {
            res_datum.set_int(v);
          } else if (ObNumberType == type) {
            number::ObNumber n;
            OZ (n.from(v, calc_buf));
            OX (res_datum.set_number(n));
          }
        } else {
          res_datum = param;
        }
      }
      break;
    }
    case PL_PLS_INTEGER:
    case PL_BINARY_INTEGER:
    case PL_NATURAL:
    case PL_NATURALN:
    case PL_POSITIVE:
    case PL_POSITIVEN:
    case PL_SIGNTYPE: {
      OZ (check_range(param, type, pls_range.range_));
      OX (res_datum = param);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pl integer type", K(pls_type), K(pls_range), K(ret));
    }
  }
  return ret;
}

int ObExprPLIntegerChecker::cg_expr(ObExprCGCtx &op_cg_ctx,
                                    const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIAllocator &alloc = *op_cg_ctx.allocator_;
  ExtraInfo *info = OB_NEWx(ExtraInfo, (&alloc), alloc, T_FUN_PL_INTEGER_CHECKER);
  if (NULL == info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_UNLIKELY(rt_expr.arg_cnt_ != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    const ObPLIntegerCheckerRawExpr &pl_expr =
    static_cast<const ObPLIntegerCheckerRawExpr&>(raw_expr);
    info->pl_integer_type_ = pl_expr.get_pl_integer_type();
    info->pl_integer_range_.set_range(pl_expr.get_lower(), pl_expr.get_upper());
    rt_expr.extra_info_ = info;
    rt_expr.eval_func_ = calc_pl_integer_checker;
  }
  return ret;
}

int ObExprPLIntegerChecker::calc_pl_integer_checker(const ObExpr &expr,
                                                    ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  const ObObjType type = expr.args_[0]->datum_meta_.type_;
  ExtraInfo *info = static_cast<ExtraInfo *>(expr.extra_info_);
  CK(OB_NOT_NULL(info));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (param->is_null()) {
    expr_datum.set_null();
  } else if (PL_PLS_INTEGER != info->pl_integer_type_
      && PL_BINARY_INTEGER != info->pl_integer_type_
      && PL_SIMPLE_INTEGER != info->pl_integer_type_
      && type != ObInt32Type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong result type", K(info->pl_integer_type_), K(type), K(ret));
  } else if (info->pl_integer_range_.get_lower() > info->pl_integer_range_.get_upper()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong pls integer range", K(ret), K(info->pl_integer_range_.range_));
  } else {
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    OZ (calc(expr_datum, type, *param, info->pl_integer_type_,
       info->pl_integer_range_, alloc_guard.get_allocator()));
  }
  return ret;
}

}
}
