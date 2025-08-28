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
 * This file contains implementation for _st_asewkb expr.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_inner_double_to_int.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
const int64_t MAX_PRECISE_DOUBLE_INT64 = 9007199254740991;  // 2^53 - 1
const int64_t MIN_PRECISE_DOUBLE_INT64 = -9007199254740991; // -2^53 + 1
const int DOUBLE_BIAS = 1023;
const int64_t DOUBLE_EXP_MASK = 0x7ff;
const int64_t DOUBLE_DIG_MASK = 0xfffffffffffff;
const double MAX_PRECISE_DOUBLE = 9007199254740991.0;  // 2^53 - 1
const double MIN_PRECISE_DOUBLE = -9007199254740991.0; // -2^53 + 1
const double MAX_DOUBLE_TO_INT64 = 9.223372036854776e+18;
const double MIN_DOUBLE_TO_INT64 = -9.223372036854776e+18;
const double MIN_DOUBLE_TO_UINT64 = 0.;
const double MAX_DOUBLE_TO_UINT64 = 1.8446744073709552e+19;
const int MAX_DOUBLE_PRINT_SIZE = 512;
ObExprInnerDoubleToInt::ObExprInnerDoubleToInt(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_INNER_DOUBLE_TO_INT, N_INNER_DOUBLE_TO_INT, 1, 
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}
/**
 * The function convert_double_to_int64_range is used to convert a double with potential precision 
 * loss into the corresponding upper and lower bounds of an int64.
 * For example, the number 9.223372036854776e+18, after being processed by this function, 
 * would be converted to start=9223372036854775296 and end=INT64_MAX. 
 * The conversion logic here cannot directly call ceil or floor because for large integers (greater than 2^53 + 1) 
 * both would return the same value.
 */
int ObExprInnerDoubleToInt::convert_double_to_int64_range(double d, int64_t &start, int64_t &end) 
{
  int64_t v = static_cast<int64_t>(d);
  if (v >= MIN_PRECISE_DOUBLE_INT64 && v <= MAX_PRECISE_DOUBLE_INT64) {
    start = v;
    end = v;
  } else {
    uint64_t num = *reinterpret_cast<uint64_t *>(&d);
    int64_t exp = ((num >> 52) & DOUBLE_EXP_MASK) - DOUBLE_BIAS;
    int64_t bits = exp - 52;
    int64_t out = (((int64_t)1 << 52) + (num & DOUBLE_DIG_MASK));
    int64_t p = (1 << (bits - 1));
    uint64_t sign = num & 0x8000000000000000;
    if (exp == 63) {
      start = 9223372036854775296;
      end = INT64_MAX;
    } else if (exp > 63) {
      start = INT64_MAX;
      end = INT64_MAX;
    } else if ((num & 1) == 1) {
      if (bits == 1) {
        out = out << bits;
        start = out;
        end = out;
      } else {
        start = ((out - 1) << bits) + p + 1;
        end = (out << bits) + p - 1;
      }
    } else {
        start = ((out - 1) << bits) + p;
        end = ((out) << bits) + p;
    }
    if (start < MAX_PRECISE_DOUBLE_INT64 + 1) {
      start = MAX_PRECISE_DOUBLE_INT64 + 1;
    }
    if (sign == 0x8000000000000000) {
      start = -start;
      end = -end;
      std::swap(start, end);
    }
  }
  return OB_SUCCESS;
}

/**
 * This function is used to convert a double to the upper and lower bounds of a uint64. 
 * The conversion logic is similar to convert_double_to_int64_range, but with a different range of values.
 */
int ObExprInnerDoubleToInt::convert_double_to_uint64_range(double d, uint64_t &start, uint64_t &end)
{
  uint64_t v = static_cast<uint64_t>(d);
  if (v >= 0 && v <= MAX_PRECISE_DOUBLE_INT64) {
    start = v;
    end = v;
  } else {
    uint64_t num = *reinterpret_cast<uint64_t *>(&d);
    int64_t exp = ((num >> 52) & DOUBLE_EXP_MASK) - DOUBLE_BIAS;
    int64_t bits = exp - 52;
    uint64_t out = (((uint64_t)1 << 52) + (num & DOUBLE_DIG_MASK));
    uint64_t p = (1 << (bits - 1));
    if (exp == 64) {
      start = 18446744073709550592u;
      end = UINT64_MAX;
    } else if (exp > 64) {
      start = UINT64_MAX;
      end = UINT64_MAX;
    } else if ((num & 1) == 1) {
      if (bits == 1) {
        out = out << bits;
        start = out;
        end = out;
      } else {
        start = ((out - 1) << bits) + p + 1;
        end = (out << bits) + p - 1;
      }
    } else {
        start = ((out - 1) << bits) + p;
        end = ((out) << bits) + p;
    }
    if (start < MAX_PRECISE_DOUBLE_INT64 + 1) {
      start = MAX_PRECISE_DOUBLE_INT64 + 1;
    }
  }
  return OB_SUCCESS;
}

/**
 * This function's purpose is to generate a double value out_d that is guaranteed to be greater than the original input value d. 
 * The algorithm achieves this by adding 1 to the least significant digit of the double's mantissa; if this causes an overflow, then 1 is added to the exponent part instead.
 * 
 * To illustrate the effect of this function with an example, suppose the input value is 3.14. After being processed by the function, the resulting value would be 3.1400000000000006.
 * 
 * Why do we need this auxiliary function? 
 * When converting a double to a decimal, because a decimal has a fractional part and the precision can vary, 
 * it's not possible to obtain an exact value as with converting to an integer. Therefore, we need to generate an imprecise range that ensures the produced range is a superset of the original one. 
 * For instance, for c1 = 3.14, the resulting range would be [3.1399999999999997; 3.1400000000000006].
 * 
 * Why doesn't the conversion function simply add 1? 
 * For numbers smaller than 2^53, adding 1 is not an issue, 
 * but for large integers, directly adding 1 doesn't cause any change to the double value. 
 * For example, 9.223372036854776e+18 + 1 is equal to 9.223372036854776e+18, showing no change.
*/
int ObExprInnerDoubleToInt::add_double_bit_1(double d, double &out_d)
{
  int ret = OB_SUCCESS;
  int64_t v = static_cast<int64_t>(d);
  uint64_t num = *reinterpret_cast<uint64_t *>(&d);
  int64_t exp = ((num >> 52) & DOUBLE_EXP_MASK);
  int64_t bits = exp - 52;
  int64_t out = (((int64_t)1 << 52) + (num & 0xfffffffffffff));
  uint64_t sign = num & 0x8000000000000000;
  out += 1;
  if ((out & 0x20000000000000) == 0x20000000000000) {
    out = out >> 1;
    exp += 1;
  }
  num = sign | (exp << 52) | (out & 0xfffffffffffff);
  out_d = *reinterpret_cast<double *>(&num);
  return ret;
}

/**
 * This function serves the opposite purpose to add_double_bit_1, yielding a double that is smaller than the input number, 
 * effectively subtracting 1 from the least significant digit of the double's precision.
 */
int ObExprInnerDoubleToInt::sub_double_bit_1(double d, double &out_d)
{
  int ret = OB_SUCCESS;
  int64_t v = static_cast<int64_t>(d);
  uint64_t num = *reinterpret_cast<uint64_t *>(&d);
  int64_t exp = ((num >> 52) & DOUBLE_EXP_MASK);
  int64_t bits = exp - 52;
  int64_t out = (((int64_t)1 << 52) + (num & 0xfffffffffffff));
  uint64_t sign = num & 0x8000000000000000;
  out -= 1;
  if ((out & 0x10000000000000) == 0) {
    out = (out << 1) + 1;
    exp -= 1;
  }
  num = sign | (exp << 52) | (out & 0xfffffffffffff);
  out_d = *reinterpret_cast<double *>(&num);
  return ret;
}

int ObExprInnerDoubleToInt::double_to_number(double in_val,
                                             ObIAllocator &alloc,
                                             number::ObNumber &number)
{
  int ret = OB_SUCCESS;
  int64_t length = 0;
  ObScale res_scale; // useless
  ObPrecision res_precision;
  ObString str;
  SMART_VAR(char[MAX_DOUBLE_PRINT_SIZE], buf) {
    MEMSET(buf, 0, MAX_DOUBLE_PRINT_SIZE);
    length = ob_gcvt_opt(in_val, OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(sizeof(buf) - 1),
                         buf, NULL, lib::is_oracle_mode(), TRUE);
    str.assign_ptr(buf, length);
    ret = number.from_sci_opt(str.ptr(), str.length(), alloc,
                              &res_precision, &res_scale);
    if (OB_NUMERIC_OVERFLOW == ret) {
      int64_t i = 0;
      while (i < str.length() && isspace(str[i])) {
        ++i;
      }
      bool is_neg = (str[i] == '-');
      int tmp_ret = OB_SUCCESS;
      const ObPrecision prec = OB_MAX_DECIMAL_PRECISION;
      const ObScale scale = 0;
      const number::ObNumber *bound_num = NULL;
      if (is_neg) {
        bound_num = &(ObNumberConstValue::MYSQL_MIN[prec][scale]);
      } else {
        bound_num = &(ObNumberConstValue::MYSQL_MAX[prec][scale]);
      }
      if (OB_ISNULL(bound_num)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bound_num is NULL", K(tmp_ret), K(ret), K(is_neg));
      } else if (OB_SUCCESS != (tmp_ret = number.from(*bound_num, alloc))) {
        LOG_WARN("copy min number failed", K(ret), K(tmp_ret), KPC(bound_num));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObExprInnerDoubleToInt::eval_inner_double_to_int(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum) {
  int ret = OB_SUCCESS;
  ObDatum* val_datum = NULL;
  double val = 0.;
  double ceil_val = 0.;
  double floor_val = 0.;
  bool is_start = (expr.extra_ & 1) == 1;
  bool is_equal = (expr.extra_ & 2) == 2;
  bool is_unsigned = (expr.extra_ & 4) == 4;
  bool is_decimal = (expr.extra_ & 8) == 8;
  if (OB_FAIL(expr.args_[0]->eval(ctx, val_datum))) {
    LOG_WARN("fail to eval conv", K(ret), K(expr));
  } else if (val_datum->is_null()) {
    expr_datum.set_null();
  } else if (OB_FALSE_IT(val = val_datum->get_double())) {
  } else if (is_decimal) {
    ObNumStackOnceAlloc tmp_alloc;
    number::ObNumber number;
    double tmp_d;
    if (val == DBL_MAX || val == DBL_MIN ||
        val == -DBL_MAX || val == -DBL_MIN) {
      // do nothing
    } else if ((val > 0. && !is_start) ||
        (val < 0. && is_start)) {
      if (OB_FAIL(add_double_bit_1(val, tmp_d))) {
        LOG_WARN("failed to add double bit 1", K(ret));
      } else {
        val = tmp_d;
      }
    } else if ((val > 0. && is_start) ||
               (val < 0. && !is_start)) {
      if (OB_FAIL(sub_double_bit_1(val, tmp_d))) {
        LOG_WARN("failed to sub double bit 1", K(ret));
      } else {
        val = tmp_d;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(double_to_number(val, tmp_alloc, number))) {
      LOG_WARN("failed to trans double to number", K(ret));
    } else {
      expr_datum.set_number(number);
    }
  } else if (!is_unsigned) {
    int64_t start = 0, end = 0;
    if (val >= MIN_PRECISE_DOUBLE && val <= MAX_PRECISE_DOUBLE) {
      ceil_val = ceill(val);
      floor_val = floorl(val);
      if (ceil_val == floor_val) {
        expr_datum.set_int(static_cast<int64_t>(val));
      } else if (is_equal) {
        expr_datum.set_int(is_start ? INT64_MAX : INT64_MIN);
      } else if (is_start) {
        expr_datum.set_int(static_cast<int64_t>(ceil_val));
      } else {
        expr_datum.set_int(static_cast<int64_t>(floor_val));
      }
    } else if (val > MAX_DOUBLE_TO_INT64) {
      if (is_equal) {
        expr_datum.set_int(is_start ? INT64_MAX : INT64_MIN);
      } else {
        expr_datum.set_int(INT64_MAX);
      }
    } else if (val < MIN_DOUBLE_TO_INT64) {
      if (is_equal) {
        expr_datum.set_int(is_start ? INT64_MAX : INT64_MIN);
      } else {
        expr_datum.set_int(INT64_MIN);
      }
    } else {
      if (OB_FAIL(convert_double_to_int64_range(val, start, end))) {
        LOG_WARN("failed to convert double to int64 range", K(ret));
      } else if (is_start) {
        expr_datum.set_int(start);
      } else {
        expr_datum.set_int(end);
      }
    }
  } else {
    uint64_t start = 0, end = 0;
    if (val >= MIN_DOUBLE_TO_UINT64 && val <= MAX_PRECISE_DOUBLE) {
      ceil_val = ceill(val);
      floor_val = floorl(val);
      if (ceil_val == floor_val) {
        expr_datum.set_uint(static_cast<uint64_t>(val));
      } else if (is_equal) {
        expr_datum.set_uint(is_start ? UINT64_MAX : 0);
      } else if (is_start) {
        expr_datum.set_uint(static_cast<uint64_t>(ceil_val));
      } else {
        expr_datum.set_uint(static_cast<uint64_t>(floor_val));
      }
    } else if (val > MAX_DOUBLE_TO_UINT64) {
      if (is_equal) {
        expr_datum.set_uint(is_start ? UINT64_MAX : 0);
      } else {
        expr_datum.set_uint(UINT64_MAX);
      }
    } else if (val < MIN_DOUBLE_TO_UINT64) {
      if (is_equal) {
        expr_datum.set_uint(is_start ? UINT64_MAX : 0);
      } else {
        expr_datum.set_uint(0);
      }
    } else {
      if (OB_FAIL(convert_double_to_uint64_range(val, start, end))) {
        LOG_WARN("failed to convert double to int64 range", K(ret));
      } else if (is_start) {
        expr_datum.set_uint(start);
      } else {
        expr_datum.set_uint(end);
      }
    }
  }
  
  return ret;
}

int ObExprInnerDoubleToInt::calc_result_type1(ObExprResType &type,
                                              ObExprResType &type1,
                                              common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *raw_expr = get_raw_expr();
  UNUSED(type_ctx);
  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op raw expr is null or param_count error", K(ret), K(raw_expr));
  } else {
    bool is_unsigned = (raw_expr->get_range_flag() & 4) == 4;
    bool is_decimal = (raw_expr->get_range_flag() & 8) == 8;
    if (is_decimal) {
      type.set_number();
    } else if (is_unsigned) {
      type.set_uint64();
    } else {
      type.set_int();
    }
    type1.set_calc_type(ObDoubleType);
  }

  return ret;
}

int ObExprInnerDoubleToInt::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                                    const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else if (OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the arg of inner double to int is null.", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = ObExprInnerDoubleToInt::eval_inner_double_to_int;
    rt_expr.extra_ = raw_expr.get_range_flag();
  }
  return ret;
}

}
}