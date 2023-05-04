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
#include "sql/engine/expr/ob_expr_to_interval.h"

#include "lib/ob_name_def.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
static bool is_mul_overflow(int64_t x, int64_t y, int64_t &result)
{
  result = x * y;
  return (x != 0) && result / x != y;
}

ObExprToYMInterval::ObExprToYMInterval(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TO_YMINTERVAL, N_TO_YMINTERVAL, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprToYMInterval::~ObExprToYMInterval()
{
}

int ObExprToYMInterval::calc_result_type1(ObExprResType &type, ObExprResType &type1, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_interval_ym();
  type.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalYMType].get_scale());
  type1.set_calc_type(ObVarcharType);
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObSEArray<ObExprResType*, 1, ObNullAllocator> params;
    OZ(params.push_back(&type1));
    ObExprResType tmp_type;
    OZ(aggregate_string_type_and_charset_oracle(*session, params, tmp_type));
    OZ(deduce_string_param_calc_type_and_charset(*session, tmp_type, params));
  }
  return ret;
}

int ObExprToYMInterval::cg_expr(ObExprCGCtx &op_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("to_yminterval expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of to_yminterval expr is null", K(ret), K(rt_expr.args_));
  } else {
      rt_expr.eval_func_ = ObExprToYMInterval::calc_to_yminterval;
  }
  return ret;
}

int ObExprToYMInterval::calc_to_yminterval(const ObExpr &expr, ObEvalCtx &ctx,
                                            ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
    LOG_WARN("calc first param failed", K(ret));
  } else if (param->is_null()) {
    expr_datum.set_null();
  } else {
    ObString str = param->get_string();
    ObIntervalYMValue value;
    ObScale max_scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalYMType].get_scale();
    ObScale scale = max_scale;
    if ((NULL == str.find('P')) ? //有P的是ISO格式
              OB_FAIL(ObTimeConverter::str_to_interval_ym(str, value, scale))
            : OB_FAIL(ObTimeConverter::iso_str_to_interval_ym(str, value))) {
      LOG_WARN("fail to convert string", K(ret), K(str));
    } else {
      expr_datum.set_interval_ym(value.get_nmonth());
    }
  }
  return ret;
}

ObExprToDSInterval::ObExprToDSInterval(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TO_DSINTERVAL, N_TO_DSINTERVAL, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprToDSInterval::~ObExprToDSInterval()
{
}

int ObExprToDSInterval::calc_result_type1(ObExprResType &type, ObExprResType &type1, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_interval_ds();
  type.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalDSType].get_scale());
  type1.set_calc_type(ObVarcharType);
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObSEArray<ObExprResType*, 1, ObNullAllocator> params;
    OZ(params.push_back(&type1));
    ObExprResType tmp_type;
    OZ(aggregate_string_type_and_charset_oracle(*session, params, tmp_type));
    OZ(deduce_string_param_calc_type_and_charset(*session, tmp_type, params));
  }
  return ret;
}

int ObExprToDSInterval::cg_expr(ObExprCGCtx &op_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("to_dsinterval expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of to_dsinterval expr is null", K(ret), K(rt_expr.args_));
  } else {
      rt_expr.eval_func_ = ObExprToDSInterval::calc_to_dsinterval;
  }
  return ret;
}

int ObExprToDSInterval::calc_to_dsinterval(const ObExpr &expr, ObEvalCtx &ctx,
                                            ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param))) {
    LOG_WARN("calc first param failed", K(ret));
  } else if (param->is_null()) {
    expr_datum.set_null();
  } else {
    ObString str = param->get_string();
    ObIntervalDSValue value;
    ObScale max_scale = ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalDSType].get_scale();
    ObScale scale = max_scale;
    if ((NULL == str.find('P')) ? //有P的是ISO格式
              OB_FAIL(ObTimeConverter::str_to_interval_ds(str, value, scale))
            : OB_FAIL(ObTimeConverter::iso_str_to_interval_ds(str, value))) {
      LOG_WARN("fail to convert string", K(ret), K(str));
    } else {
      expr_datum.set_interval_ds(value);
    }
  }
  return ret;
}

ObExprNumToYMInterval::ObExprNumToYMInterval(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_NUMTOYMINTERVAL, N_NUMTOYMINTERVAL, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprNumToYMInterval::~ObExprNumToYMInterval()
{
}

int ObExprNumToYMInterval::calc_result_type2(ObExprResType &type,
                                             ObExprResType &type1,
                                             ObExprResType &type2,
                                             ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_interval_ym();
  type.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalYMType].get_scale());
  type1.set_calc_type(ObNumberType);
  type1.set_calc_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
  type2.set_calc_type(ObVarcharType);
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObSEArray<ObExprResType*, 1, ObNullAllocator> params;
    OZ(params.push_back(&type2));
    ObExprResType tmp_type;
    OZ(aggregate_string_type_and_charset_oracle(*session, params, tmp_type));
    OZ(deduce_string_param_calc_type_and_charset(*session, tmp_type, params));
  }
  return ret;
}

template <class T>
int ObExprNumToYMInterval::calc_result_common(const T &obj1,
                                              const T &obj2,
                                              ObIAllocator &calc_buf,
                                              ObIntervalYMValue &ym_value)
{
  int ret = OB_SUCCESS;
  const number::ObNumber num_n = obj1.get_number();
  ObString unit = obj2.get_string();
  int64_t nmonth = 0;
  int64_t mul_f = 0;

  unit = unit.trim();
  if (0 == unit.case_compare(ob_date_unit_type_str(DATE_UNIT_YEAR))) {
    mul_f = ObIntervalYMValue::MONTHS_IN_YEAR;
  } else if (0 == unit.case_compare(ob_date_unit_type_str(DATE_UNIT_MONTH))) {
    mul_f = 1;
  } else {
    ret = OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION;
    LOG_WARN("illegal argument for function", K(ret), K(unit));
  }
  if (OB_SUCC(ret)) {
    int64_t n = 0;
    if (num_n.is_valid_int64(n)) {
      //如果是整数，短路径
      if (OB_UNLIKELY(is_mul_overflow(mul_f, n, nmonth))) {
        ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
        LOG_WARN("mul size overflow", K(ret));
      }
    } else {
      number::ObNumber month_per_year;
      number::ObNumber num_result;
      ObNumStackOnceAlloc tmp_alloc;
      if (mul_f == 1) {
        if (OB_FAIL(num_result.from(num_n, tmp_alloc))) {
          LOG_WARN("copy num failed", K(ret));
        }
      } else {
        if (OB_FAIL(month_per_year.from(mul_f, calc_buf))) {
          LOG_WARN("number from failed", K(ret));
        } else if (OB_FAIL(num_n.mul(month_per_year, num_result, calc_buf))) {
          LOG_WARN("number mul failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(num_result.round(0))) {
          LOG_WARN("number found failed", K(ret));
        } else if (OB_UNLIKELY(!num_result.is_valid_int64(nmonth))) {
          ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
          LOG_WARN("number should be valid int64", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ym_value.nmonth_ = nmonth;
    if (OB_FAIL(ym_value.validate())) {
      LOG_WARN("fail to validate interval", K(ret), K(ym_value));
    }
  }
  return ret;
}

int ObExprNumToYMInterval::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("num_to_yminterval expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])
             || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of num_to_yminterval expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprNumToYMInterval::calc_num_to_yminterval;
  }
  return ret;
}
int ObExprNumToYMInterval::calc_num_to_yminterval(const ObExpr &expr, ObEvalCtx &ctx,
                                                  ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param1 = NULL;
  ObDatum *param2 = NULL;
  ObIntervalYMValue ym_value;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("calc first param failed", K(ret));
  } else if (param1->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, param2))) {
    LOG_WARN("calc second param failed", K(ret));
  } else if (param2->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(calc_result_common(*param1, *param2, alloc_guard.get_allocator(), ym_value))) {
    LOG_WARN("calc result failed", K(ret));
  } else {
    expr_datum.set_interval_ym(ym_value.get_nmonth());
  }
  return ret;
}

ObExprNumToDSInterval::ObExprNumToDSInterval(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_NUMTODSINTERVAL, N_NUMTODSINTERVAL, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprNumToDSInterval::~ObExprNumToDSInterval()
{
}

int ObExprNumToDSInterval::calc_result_type2(ObExprResType &type,
                                             ObExprResType &type1,
                                             ObExprResType &type2,
                                             ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_interval_ds();
  type.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObIntervalDSType].get_scale());
  type1.set_calc_type(ObNumberType);
  type1.set_calc_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
  type2.set_calc_type(ObVarcharType);
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObSEArray<ObExprResType*, 1, ObNullAllocator> params;
    OZ(params.push_back(&type2));
    ObExprResType tmp_type;
    OZ(aggregate_string_type_and_charset_oracle(*session, params, tmp_type));
    OZ(deduce_string_param_calc_type_and_charset(*session, tmp_type, params));
  }
  return ret;
}

template <class R, class T>
int ObExprNumToDSInterval::calc_result_common(R &result,
                                              const T &obj1,
                                              const T &obj2,
                                              ObIAllocator &calc_buf)
{
  int ret = OB_SUCCESS;
  static int64_t mul_f[DATE_UNIT_DAY - DATE_UNIT_SECOND + 1] = {
    1,
    ObIntervalDSValue::SECONDS_IN_MINUTE,
    ObIntervalDSValue::SECONDS_IN_MINUTE * ObIntervalDSValue::MINUTES_IN_HOUR,
    ObIntervalDSValue::SECONDS_IN_MINUTE * ObIntervalDSValue::MINUTES_IN_HOUR * ObIntervalDSValue::HOURS_IN_DAY,
  };
  
  ObString unit = obj2.get_string();
  number::ObNumber num_n = obj1.get_number();
  int64_t nsecond = 0;
  int64_t fs = 0;

  int matched_idx = OB_INVALID_INDEX;
  unit = unit.trim();
  for (int i = DATE_UNIT_SECOND; i <= DATE_UNIT_DAY; ++i) {
    if (0 == unit.case_compare(ob_date_unit_type_str(static_cast<ObDateUnitType>(i)))) {
      matched_idx = i - DATE_UNIT_SECOND;
      break;
    }
  }

  if (OB_UNLIKELY(OB_INVALID_INDEX == matched_idx)) {
    ret = OB_ERR_ILLEGAL_ARGUMENT_FOR_FUNCTION;
    LOG_WARN("illegal argument for function", K(ret));
  } else {
    int64_t n = 0;
    if (num_n.is_valid_int64(n)) {
      //如果是整数，短路径
      if (OB_UNLIKELY(is_mul_overflow(mul_f[matched_idx], n, nsecond))) {
        ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
        LOG_WARN("mul size overflow", K(ret));
      }
    } else {
      number::ObNumber num_mul_f;
      number::ObNumber num_nsecond;

      if (OB_FAIL(num_mul_f.from(mul_f[matched_idx], calc_buf))) {
        LOG_WARN("number from failed", K(ret));
      } else if (OB_FAIL(num_n.mul(num_mul_f, num_nsecond, calc_buf))) {
        LOG_WARN("number mul failed", K(ret));
      } else if (OB_FAIL(num_nsecond.round(MAX_SCALE_FOR_ORACLE_TEMPORAL))) {
        LOG_WARN("number round failed", K(ret));
      } else if (OB_UNLIKELY(!num_nsecond.is_int_parts_valid_int64(nsecond, fs))) {
        ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
        LOG_WARN("number should be valid int64", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (nsecond < 0) {
      fs = -fs;
    }
    ObIntervalDSValue value = ObIntervalDSValue(nsecond, static_cast<int32_t>(fs));
    if (OB_FAIL(value.validate())) {
      LOG_WARN("invalid interval result", K(ret), K(value));
    } else {
      result.set_interval_ds(value);
    }
  }

  return ret;
}

int ObExprNumToDSInterval::cg_expr(ObExprCGCtx &op_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("num_to_dsinterval expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])
             || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of num_to_dsinterval expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprNumToDSInterval::calc_num_to_dsinterval;
  }
  return ret;
}
int ObExprNumToDSInterval::calc_num_to_dsinterval(const ObExpr &expr, ObEvalCtx &ctx,
                                                  ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param1 = NULL;
  ObDatum *param2 = NULL;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("calc first param failed", K(ret));
  } else if (param1->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, param2))) {
    LOG_WARN("calc second param failed", K(ret));
  } else if (param2->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(calc_result_common(expr_datum, *param1, *param2, alloc_guard.get_allocator()))) {
    LOG_WARN("calc result failed", K(ret));
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
