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
#include "lib/ob_name_def.h"
#include "sql/engine/expr/ob_expr_time_format.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprTimeFormat::ObExprTimeFormat(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_TIME_FORMAT, N_TIME_FORMAT, 2, VALID_FOR_GENERATED_COL)
{}

ObExprTimeFormat::~ObExprTimeFormat()
{
}

int ObExprTimeFormat::calc_result_type2(ObExprResType &type,
                                        ObExprResType &time,
                                        ObExprResType &format,
                                        common::ObExprTypeCtx &type_ctx) const
{
  int ret = common::OB_SUCCESS;
  calc_temporal_format_result_length(type, format);
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);

  time.set_calc_type(ObTimeType);
  format.set_calc_type(ObVarcharType);
  format.set_calc_collation_type(type.get_collation_type());
  // result is null if cast first param to ObTimeType failed.
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
  return ret;
}


int ObExprTimeFormat::time_to_str_format(const int64_t &time_value, const ObString &format,
                                         char *buf, int64_t buf_len, int64_t &pos, bool &res_null)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (ObTimeConverter::time_to_ob_time(time_value, ob_time)) {
    LOG_WARN("time to ob time failed", K(ret), K(time_value));
  } else if (OB_ISNULL(format.ptr()) || OB_ISNULL(buf)
            || OB_UNLIKELY(format.length() <= 0 || buf_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("format or output string is invalid", K(ret), K(format), K(buf), K(buf_len));
  } else if (IS_NEG_TIME(ob_time.mode_) && OB_FAIL(databuff_printf(buf, buf_len, pos, "-"))) {
    LOG_WARN("print - failed", K(ret));
  } else {
    const char *format_ptr = format.ptr();
    const char *end_ptr = format.ptr() + format.length();
    const int32_t *parts = ob_time.parts_;
    //used for am/pm conversation in order to avoid if-else tests.
    int hour12 = ((parts[DT_HOUR] + 11) % 12) + 1;
    while (format_ptr < end_ptr && OB_SUCCESS == ret && !res_null) {
      if ('%' == *format_ptr) {
        format_ptr++;
        if (format_ptr >= end_ptr) {
          if (pos >= buf_len) {
            ret = OB_SIZE_OVERFLOW;
            break;
          }
          buf[pos++] = '%';
          break;
        }
        switch (*format_ptr) {
          case 'f': { //Microseconds (000000..999999), include '-' if value is negative.
            ret = ObTimeConverter::data_fmt_nd(buf, buf_len, pos, 6, parts[DT_USEC]);
            break;
          }
          case 'h': //Hour (01..12)
          case 'I': { //Hour (01..12)
            int hour = hour12;
            ret = ObTimeConverter::data_fmt_nd(buf, buf_len, pos, 2, hour);
            break;
          }
          case 'i': { //Minutes, numeric (00..59)
            ret = ObTimeConverter::data_fmt_nd(buf, buf_len, pos, 2, parts[DT_MIN]);
            break;
          }
          case 'H': { //Hour
            if (parts[DT_HOUR] < 100) {
              ret = ObTimeConverter::data_fmt_nd(buf, buf_len, pos, 2, parts[DT_HOUR]);
            } else {
              ret = ObTimeConverter::data_fmt_nd(buf, buf_len, pos, 3, parts[DT_HOUR]);
            }
            break;
          }
          case 'k': { //Hour
            ret = ObTimeConverter::data_fmt_d(buf, buf_len, pos, parts[DT_HOUR]);
            break;
          }
          case 'l': { //Hour (1..12)
            int hour = hour12;
            ret = ObTimeConverter::data_fmt_d(buf, buf_len, pos, hour);
            break;
          }
          case 'p': { //AM or PM
            const char *ptr = (parts[DT_HOUR] % 24) < 12 ? "AM" : "PM";
            ret = ObTimeConverter::data_fmt_s(buf, buf_len, pos, ptr);
            break;
          }
          case 'r': { //Time, 12-hour (hh:mm:ss followed by AM or PM)
            const char *ptr = (parts[DT_HOUR] % 24) < 12 ? "AM" : "PM";
            ret = databuff_printf(buf, buf_len, pos, "%02d:%02d:%02d %s", hour12, parts[DT_MIN], parts[DT_SEC], ptr);
            break;
          }
          case 'S': //Seconds (00..59)
          case 's': { //Seconds (00..59)
            ret = ObTimeConverter::data_fmt_nd(buf, buf_len, pos, 2, parts[DT_SEC]);
            break;
          }
          case 'T': { //Time (hh:mm:ss)
            if (parts[DT_HOUR] < 100) {
              ret = ObTimeConverter::data_fmt_nd(buf, buf_len, pos, 2, parts[DT_HOUR]);
            } else {
              ret = ObTimeConverter::data_fmt_nd(buf, buf_len, pos, 3, parts[DT_HOUR]);
            }
            if (OB_SUCC(ret)) {
              ret = databuff_printf(buf, buf_len, pos, ":%02d:%02d", parts[DT_MIN], parts[DT_SEC]);
            }
            break;
          }
          case 'c':   //Month, numeric (0..12)
          case 'e': { //Day of the month, numeric (0..31)
            ret = databuff_printf(buf, buf_len, pos, "0");
            break;
          }
          case 'd':   //Day of the month, numeric (00..31)
          case 'm':   //Month, numeric (00..12)
          case 'y': { //Year, numeric (two digits)
            ret = databuff_printf(buf, buf_len, pos, "00");
            break;
          }
          case 'Y': { //Year, numeric, four digits
            ret = databuff_printf(buf, buf_len, pos, "0000");
            break;
          }
          case 'a':   //Abbreviated weekday name (Sun..Sat)
          case 'b':   //Abbreviated month name (Jan..Dec)
          case 'D':   //Day of the month with English suffix (0th, 1st, 2nd, 3rd...)
          case 'j':   //Day of year (001..366)
          case 'M':   //Month name (January..December)
          case 'U':   //Week (00..53), where Sunday is the first day of the week
          case 'u':   //Week (00..53), where Monday is the first day of the week
          case 'V':   //Week (01..53), where Sunday is the first day of the week; used with %X
          case 'v':   //Week (01..53), where Monday is the first day of the week; used with %x
          case 'W':   //Weekday name (Sunday..Saturday)
          case 'w':   //Day of the week (0=Sunday..6=Saturday)
          case 'X':   //Year for the week where Sunday is the first day of the week
          case 'x': { //Year for the week, where Monday is the first day of the week
            res_null = true;
            break;
          }
          case '%': { //A literal "%" character
            if (pos >= buf_len) {
              ret = OB_SIZE_OVERFLOW;
              break;
            }
            buf[pos++] = '%';
            break;
          }
          default: {
            if (pos >= buf_len) {
              ret = OB_SIZE_OVERFLOW;
              break;
            }
            buf[pos++] = *format_ptr;
            break;
          }
        }
        if (OB_SUCC(ret)) {
          format_ptr++;
        } else {
          LOG_WARN("print failed", K(ret), K(*format_ptr), K(time_value), K(ob_time));
        }
      } else if (pos >= buf_len) {
        ret = OB_SIZE_OVERFLOW;
        break;
      } else {
        buf[pos++] = *(format_ptr++);
      }
    }
  }
  return ret;
}

int ObExprTimeFormat::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("date_format expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])
             || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of date_format expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprTimeFormat::calc_time_format;
  }
  return ret;
}

int ObExprTimeFormat::calc_time_format(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  ObDatum *time = NULL;
  ObDatum *format = NULL;
  bool res_null = false;
  if (OB_FAIL(expr.eval_param_value(ctx, time, format))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (time->is_null() || format->is_null()) {
    expr_datum.set_null();
  } else if (OB_UNLIKELY(format->get_string().empty())) {
    expr_datum.set_null();
  } else if (FALSE_IT(buf_len = format->get_string().length() * OB_TEMPORAL_BUF_SIZE_RATIO)) {
  } else if (!ob_is_text_tc(expr.datum_meta_.type_)) {
    if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no more memory to alloc for buf", K(ret), K(buf_len));
    } else if (OB_FAIL(time_to_str_format(time->get_time(),
                                          format->get_string(),
                                          buf,
                                          buf_len,
                                          pos,
                                          res_null))) {
      LOG_WARN("failed to convert ob time to str with format");
    } else if (res_null) {
      expr_datum.set_null();
    } else {
      expr_datum.set_string(buf, static_cast<int32_t>(pos));
    }
  } else { // text tc
    ObTextStringDatumResult output_result(expr.datum_meta_.type_, &expr, &ctx, &expr_datum);
    if (OB_FAIL(output_result.init(buf_len))) {
      LOG_WARN("init lob result failed", K(ret), K(buf_len));
    } else if (OB_FAIL(output_result.get_reserved_buffer(buf, buf_len))) {
      LOG_WARN("get reserved buffer for blob failed", K(ret), K(buf_len));
    } else if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no more memory to alloc for buf", K(ret), K(buf_len));
    } else if (OB_FAIL(time_to_str_format(time->get_time(),
                                          format->get_string(),
                                          buf,
                                          buf_len,
                                          pos,
                                          res_null))) {
      LOG_WARN("failed to convert ob time to lob str with format");
    } else if (res_null) {
      expr_datum.set_null();
    } else if (OB_FAIL(output_result.lseek(pos, 0))){
      LOG_WARN("lseek text or string result failed", K(ret), K(pos));
    } else {
      output_result.set_result();
    }
  }
  return ret;
}

}
}
