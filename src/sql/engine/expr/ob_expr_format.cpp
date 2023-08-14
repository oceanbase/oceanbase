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

#define USING_LOG_PREFIX  SQL_ENG

#include "ob_expr_format.h"
#include "lib/charset/ob_charset.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{

const int64_t FORMAT_MAX_DECIMALS = 30;
const int64_t DECIMAL_NOT_SPECIFIED = 31;
const int64_t MAX_TINY_TEXT_BUFFER_SIZE = 373;
const int64_t MAX_FLOAT_BUFFER_SIZE = 49;
const int64_t MAX_DOUBLE_BUFFER_SIZE = 62;
const int64_t MAX_VARCHAR_BUFFER_SIZE = 359;
const int64_t MAX_FORMAT_BUFFER_SIZE = 512;
const int64_t MAX_TEXT_BUFFER_SIZE = 16359;

ObExprFormat::ObExprFormat(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_FORMAT, N_FORMAT, TWO_OR_THREE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprFormat::~ObExprFormat()
{
}

int ObExprFormat::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *type_array,
                                    int64_t params_count,
                                    ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == type_array || (params_count != 2 && params_count != 3))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(type_array), K(params_count));
  } else if (params_count == 3) {
    //to do, format function support a optional locale parameter
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("format function not support an optional locale parameter", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "optional locale parameter of format");
  } else if (OB_FAIL(calc_result_type(type, type_array))) {
    LOG_WARN("failed to calc result type", K(ret));
  } else {
    type.set_default_collation_type();
    type.set_collation_level(CS_LEVEL_COERCIBLE);
    CK (OB_NOT_NULL(type_ctx.get_session()));
    ObExprOperator::calc_result_flagN(type, type_array, params_count);
    // 利用表达式框架，第二个参数转换成int类型处理
    type_array[1].set_calc_type(ObIntType);
  }
  return ret;
}

int ObExprFormat::get_origin_param_type(ObExprResType &ori_type) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  if (OB_ISNULL(expr = get_raw_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get_raw_expr", K(ret));
  } else if (expr->get_children_count() >= 1 && OB_NOT_NULL(expr = expr->get_param_expr(0))
             && expr->get_expr_type() == T_FUN_SYS_CAST && CM_IS_IMPLICIT_CAST(expr->get_extra())) {
    do {
      if (expr->get_children_count() >= 1
          && OB_ISNULL(expr = expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get_param_expr", K(ret));
      }
    } while (OB_SUCC(ret) && T_FUN_SYS_CAST == expr->get_expr_type()
             && CM_IS_IMPLICIT_CAST(expr->get_extra()));
    if (OB_SUCC(ret)) {
      ori_type = expr->get_result_type();
    }
  }
  return ret;
}

int ObExprFormat::calc_result_type(ObExprResType &type, ObExprResType *type_array) const
{
  int ret = OB_SUCCESS;
  ObExprResType ori_type = type_array[0];
  if (OB_FAIL(get_origin_param_type(ori_type))) {
    LOG_WARN("fail to get_origin_param_type", K(ret));
  } else {
    const ObObjType obj_type = ori_type.get_type();
    if (ob_is_integer_type(obj_type)) {
      int64_t max_char_length = type_array[0].get_length();
      int64_t max_sep_count = (max_char_length / 3) + /*decimals*/1 + /*sign*/1;
      type.set_varchar();
      type.set_length(max_char_length + max_sep_count + DECIMAL_NOT_SPECIFIED);
      type_array[0].set_calc_type(ObNumberType);
    } else {
      switch (obj_type) {
        case ObNullType: {
          type.set_null();
          break;
        }
        case ObFloatType:
        case ObUFloatType: {
          type.set_varchar();
          type.set_length(MAX_FLOAT_BUFFER_SIZE);
          break;
        }
        case ObDoubleType:
        case ObUDoubleType: {
          type.set_varchar();
          type.set_length(MAX_DOUBLE_BUFFER_SIZE);
          break;
        }
        case ObNumberType:
        case ObUNumberType: {
          int64_t max_char_length = type_array[0].get_length();
          int64_t max_sep_count = (max_char_length / 3) + /*decimals*/1 + /*sign*/1;
          type.set_varchar();
          type.set_length(max_char_length + max_sep_count + DECIMAL_NOT_SPECIFIED);
          break;
        }
        case ObBitType:
        case ObEnumType:
        case ObSetType:
        case ObDateTimeType:
        case ObTimestampType:
        case ObDateType:
        case ObTimeType:
        case ObYearType: {
          int64_t max_char_length = type_array[0].get_length();
          int64_t max_sep_count = (max_char_length / 3) + /*decimals*/1 + /*sign*/1;
          type.set_length(max_char_length + max_sep_count + DECIMAL_NOT_SPECIFIED);
          type.set_varchar();
          type_array[0].set_calc_type(ObDoubleType);
          break;
        }
        case ObTinyTextType:{
          type.set_varchar();
          type.set_length(MAX_TINY_TEXT_BUFFER_SIZE);
          type_array[0].set_calc_type(ObDoubleType);
          break;
        }
        case ObTextType:
        case ObMediumTextType:
        case ObLongTextType: {
          type.set_type(ObLongTextType);
          type_array[0].set_calc_type(ObDoubleType);
          break;
        }
        case ObVarcharType:
        case ObCharType: {
          int32_t length = ori_type.get_length();
          if (length <= MAX_VARCHAR_BUFFER_SIZE) {
            type.set_varchar();
            int64_t max_sep_count = (length / 3) + /*decimals*/1 + /*sign*/1;
            type.set_length(length + max_sep_count + DECIMAL_NOT_SPECIFIED);
          } else if (length <=  MAX_TEXT_BUFFER_SIZE) {
            type.set_type(ObTextType);
          } else {
            type.set_type(ObLongTextType);
          }
          type_array[0].set_calc_type(ObDoubleType);
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("input type not supported", K(ret), K(obj_type));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "The input type of the FORMAT function");
          break;
        }
      }
      if (OB_SUCC(ret) && ob_is_text_tc(type.get_type())) {
        const int32_t mbmaxlen = 4;
        const int32_t default_text_length =
          ObAccuracy::DDL_DEFAULT_ACCURACY[type.get_type()].get_length() / mbmaxlen;
        type.set_length(default_text_length);
      }
    }
  }
  return ret;
}

int64_t ObExprFormat::get_format_scale(int64_t scale)
{
  if (scale <= 0) {
    scale = 0;
  } else if (scale >= FORMAT_MAX_DECIMALS) {
    scale = FORMAT_MAX_DECIMALS;
  }
  return scale;
}

int ObExprFormat::build_format_str(char *buf,
                                   const ObLocale &locale,
                                   int64_t scale,
                                   ObString &num_str)
{
  int ret = OB_SUCCESS;
  int64_t str_length = num_str.length();
  // number of characters used to represent the decimals, including '.'
  int64_t decimal_length = scale ? scale + 1 : 0;
  // integer part grouping length
  int64_t grouping_length = locale.grouping_[0];
  bool is_negative = ('-' == *num_str.ptr());
  // src points to the last number in the integer part
  const char *src = num_str.ptr() + str_length - decimal_length - 1;
  const char *integer_part_begin =  num_str.ptr();
  if (is_negative) { ++integer_part_begin; }
  char *dst = buf + 2 * FLOATING_POINT_BUFFER;
  char *start_dst = dst;

  // put the fractional part
  if (scale) {
    dst -= (scale + 1);
    // decimal symbol, default is "."
    *dst = locale.decimal_point_;
    MEMCPY(dst + 1, src + 2, scale);
  }
  if (grouping_length > 0 && str_length >= decimal_length + grouping_length + 1) {
    for (int64_t count = grouping_length; src >= integer_part_begin; count--) {
      if (count == 0) {
        *--dst = locale.thousand_sep_;
        if (locale.grouping_[1]) {
          grouping_length = locale.grouping_[1];
        }
        count = grouping_length;
      }
      *--dst = *src--;
    }
    if (is_negative) {
      *--dst = '-';
    }
    size_t result_length = start_dst - dst;
    num_str.assign_ptr(dst, result_length);
  } else if (decimal_length && locale.decimal_point_ != '.') {
    if (decimal_length > str_length) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid str.", K(ret), K(scale), K(num_str));
    } else {
      num_str.ptr()[str_length - decimal_length] = locale.decimal_point_;
    }
  }
  return ret;
}

int ObExprFormat::convert_num_to_str(const ObObjType &obj_type,
                                     const ObDatum &x_datum,
                                     char *buf,
                                     int64_t buf_len,
                                     int64_t scale,
                                     ObString &num_str)
{
  int ret = OB_SUCCESS;
  int64_t str_len = 0;
  if (ObDoubleType == obj_type || ObUDoubleType == obj_type) {
    str_len = ob_fcvt(ObExprUtil::round_double(x_datum.get_double(), scale), scale, buf_len,
                      buf, NULL);
  } else if (ObFloatType == obj_type || ObUFloatType == obj_type) {
    str_len = ob_fcvt(ObExprUtil::round_double(x_datum.get_float(), scale), scale, buf_len,
                      buf, NULL);
  } else {
    number::ObNumber num = x_datum.get_number();
    number::ObNumber nmb;
    ObNumStackOnceAlloc tmp_alloc;
    if (OB_FAIL(nmb.from(num, tmp_alloc))) {
      LOG_WARN("copy number failed.", K(ret), K(num));
    } else if (OB_FAIL(nmb.round(scale))) {
      LOG_WARN("round failed.", K(ret), K(num.format()), K(scale));
    } else if (OB_FAIL(nmb.format_v2(buf, buf_len, str_len, scale))) {
      LOG_WARN("fail to convert number to string", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    num_str.assign_ptr(buf, static_cast<int32_t>(str_len));
    LOG_DEBUG("convert_num_to_str", K(ret), K(num_str), K(str_len));
  }
  return ret;
}

int ObExprFormat::calc_format_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  int64_t scale = 0;
  ObDatum *x_datum = NULL;
  ObDatum *d_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, d_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if ((OB_NOT_NULL(x_datum) && x_datum->is_null())
            || (OB_NOT_NULL(d_datum) && d_datum->is_null())) {
    res_datum.set_null();
  } else {
    scale = get_format_scale(d_datum->get_int());
    ObString str;
    ObString res_str;
    ObLocale locale;
    const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
    char *tmp_buf = NULL;
    char *res_buf = NULL;
    int64_t buf_len = MAX_FORMAT_BUFFER_SIZE + 2 * FLOATING_POINT_BUFFER + 1;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_ISNULL(tmp_buf = static_cast<char *>(
                            alloc_guard.get_allocator().alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate memory", K(buf_len), K(ret));
    } else if (OB_FAIL(convert_num_to_str(x_type, *x_datum, tmp_buf, MAX_FORMAT_BUFFER_SIZE,
                                          scale, str))) {
      LOG_WARN("fail to convert num to str", K(ret));
    } else {
      tmp_buf = tmp_buf + MAX_FORMAT_BUFFER_SIZE;
      if (OB_FAIL(build_format_str(tmp_buf, locale, scale, str))) {
        LOG_WARN("fail to build format str", K(ret));
      } else if (!ob_is_text_tc(expr.datum_meta_.type_)) {
        if (OB_ISNULL(res_buf = expr.get_str_res_mem(ctx, str.length()))) {
          LOG_ERROR("fail to allocate memory", K(buf_len), K(ret));
        } else {
          MEMCPY(res_buf, str.ptr(), str.length());
          res_str.assign_ptr(res_buf, str.length());
          res_datum.set_string(res_str);
        }
      } else { // text tc
        ObTextStringDatumResult text_res(expr.datum_meta_.type_, &expr, &ctx, &res_datum);
        if (OB_FAIL(text_res.init(str.length()))) {
          LOG_WARN("init lob result failed", K(ret), K(str.length()));
        } else if (OB_FAIL(text_res.append(str.ptr(), str.length()))) {
          LOG_WARN("failed to append realdata", K(ret), K(str), K(text_res));
        } else {
          text_res.set_result();
        }
      }
    }
  }
  return ret;
}

int ObExprFormat::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_format_expr;
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
