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
#include "sql/engine/expr/ob_expr_date_format.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/ob_date_unit_type.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprDateFormat::ObExprDateFormat(ObIAllocator& alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_DATE_FORMAT, N_DATE_FORMAT, 2)
{}

ObExprDateFormat::~ObExprDateFormat()
{}

int ObExprDateFormat::calc_result2(ObObj& result, const ObObj& date, const ObObj& format, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  char* buf = NULL;
  int64_t buf_len = OB_MAX_DATE_FORMAT_BUF_LEN;
  int64_t pos = 0;
  bool res_null = false;
  if (OB_UNLIKELY(ObNullType == date.get_type() || ObNullType == format.get_type())) {
    result.set_null();
  } else if (OB_UNLIKELY(ObStringTC != format.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("DATE_FORMAT() expected a string as format argument");
    check_reset_status(expr_ctx, ret, result);
  } else if (OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(expr_ctx.my_session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the pointer is null");
  } else if (OB_ISNULL(buf = static_cast<char*>(expr_ctx.calc_buf_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no more memory to alloc for buf");
  } else if (OB_FAIL(ob_obj_to_ob_time_with_date(date, get_timezone_info(expr_ctx.my_session_), ob_time))) {
    LOG_WARN("failed to convert obj to ob time");
    check_reset_status(expr_ctx, ret, result);
  } else if (ObTimeType == date.get_type() && OB_FAIL(set_cur_date(get_timezone_info(expr_ctx.my_session_), ob_time))) {
    LOG_WARN("failed to set current date to ob time");
    check_reset_status(expr_ctx, ret, result);
  } else if (OB_UNLIKELY(format.get_string().empty())) {
    result.set_null();
  } else if (OB_FAIL(
                 ObTimeConverter::ob_time_to_str_format(ob_time, format.get_string(), buf, buf_len, pos, res_null))) {
    LOG_WARN("failed to convert ob time to str with format");
  } else if (res_null) {
    result.set_null();
  } else {
    result.set_varchar(buf, static_cast<int32_t>(pos));
    result.set_collation(result_type_);
  }
  return ret;
}

int ObExprDateFormat::set_cur_date(const ObTimeZoneInfo* tz_info, ObTime& ob_time)
{
  int ret = OB_SUCCESS;
  ObTime cur_date;
  if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(ObTimeUtility::current_time(), tz_info, cur_date))) {
    LOG_WARN("failed to convert current datetime to ob time");
  } else {
    ob_time.parts_[DT_YEAR] = cur_date.parts_[DT_YEAR];
    ob_time.parts_[DT_MON] = cur_date.parts_[DT_MON];
    ob_time.parts_[DT_MDAY] = cur_date.parts_[DT_MDAY];
    ob_time.parts_[DT_DATE] = cur_date.parts_[DT_DATE];
    ob_time.parts_[DT_YDAY] = cur_date.parts_[DT_YDAY];
    ob_time.parts_[DT_WDAY] = cur_date.parts_[DT_WDAY];
  }
  return ret;
}

void ObExprDateFormat::check_reset_status(ObExprCtx& expr_ctx, int& ret, ObObj& result)
{
  if (CM_IS_WARN_ON_FAIL(expr_ctx.cast_mode_)) {
    ret = OB_SUCCESS;
    result.set_null();
  }
}

int ObExprDateFormat::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("date_format expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of date_format expr is null", K(ret), K(rt_expr.args_));
  } else if (ObStringTC != ob_obj_type_class(rt_expr.args_[1]->datum_meta_.type_) &&
             ObNullType != rt_expr.args_[1]->datum_meta_.type_) {
    rt_expr.eval_func_ = ObExprDateFormat::calc_date_format_invalid;
  } else {
    rt_expr.eval_func_ = ObExprDateFormat::calc_date_format;
  }
  return ret;
}

int ObExprDateFormat::calc_date_format(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  char* buf = NULL;
  int64_t buf_len = OB_MAX_DATE_FORMAT_BUF_LEN;
  int64_t pos = 0;
  const ObSQLSessionInfo* session = NULL;
  ObDatum* date = NULL;
  ObDatum* format = NULL;
  uint64_t cast_mode = 0;
  bool res_null = false;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret), K(session));
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode))) {
    LOG_WARN("get default cast mode failed", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, date, format))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (date->is_null() || format->is_null()) {
    expr_datum.set_null();
  } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no more memory to alloc for buf");
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(*date,
                 expr.args_[0]->datum_meta_.type_,
                 get_timezone_info(session),
                 ob_time,
                 get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx())))) {
    LOG_WARN("failed to convert datum to ob time");
    if (CM_IS_WARN_ON_FAIL(cast_mode) && OB_ALLOCATE_MEMORY_FAILED != ret) {
      ret = OB_SUCCESS;
      expr_datum.set_null();
    }
  } else if (ObTimeType == expr.args_[0]->datum_meta_.type_ &&
             OB_FAIL(set_cur_date(get_timezone_info(session), ob_time))) {
    LOG_WARN("failed to set current date to ob time");
    if (CM_IS_WARN_ON_FAIL(cast_mode) && OB_ALLOCATE_MEMORY_FAILED != ret) {
      ret = OB_SUCCESS;
      expr_datum.set_null();
    }
  } else if (OB_UNLIKELY(format->get_string().empty())) {
    expr_datum.set_null();
  } else if (OB_FAIL(
                 ObTimeConverter::ob_time_to_str_format(ob_time, format->get_string(), buf, buf_len, pos, res_null))) {
    LOG_WARN("failed to convert ob time to str with format");
  } else if (res_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_string(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObExprDateFormat::calc_date_format_invalid(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  UNUSED(expr);
  int ret = OB_SUCCESS;
  expr_datum.set_null();
  uint64_t cast_mode = 0;
  const ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret), K(session));
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode))) {
    LOG_WARN("get default cast mode failed", K(ret));
  } else if (!CM_IS_WARN_ON_FAIL(cast_mode)) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

const char* ObExprGetFormat::FORMAT_STR[FORMAT_MAX] =
{
  "EUR",
  "INTERNAL",
  "ISO",
  "JIS",
  "USA"
};

const char* ObExprGetFormat::DATE_FORMAT[FORMAT_MAX + 1] =
{
  "%d.%m.%Y",
  "%Y%m%d",
  "%Y-%m-%d",
  "%Y-%m-%d",
  "%m.%d.%Y",
  "invalid"
};

const char* ObExprGetFormat::TIME_FORMAT[FORMAT_MAX + 1] =
{
  "%H.%i.%s",
  "%H%i%s",
  "%H:%i:%s",
  "%H:%i:%s",
  "%h:%i:%s %p",
  "invalid"
};

const char* ObExprGetFormat::DATETIME_FORMAT[FORMAT_MAX + 1] =
{
  "%Y-%m-%d %H.%i.%s",
  "%Y%m%d%H%i%s",
  "%Y-%m-%d %H:%i:%s",
  "%Y-%m-%d %H:%i:%s",
  "%Y-%m-%d %H.%i.%s",
  "invalid"
};

ObExprGetFormat::ObExprGetFormat(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_GET_FORMAT, N_GET_FORMAT, 2)
{
}

ObExprGetFormat::~ObExprGetFormat()
{
}

inline int ObExprGetFormat::calc_result_type2(ObExprResType &type,
                                               ObExprResType &unit,
                                               ObExprResType &format,
                                               common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(unit);
  int ret = common::OB_SUCCESS;
//  common::ObCollationType collation_connection = common::CS_TYPE_INVALID;
  type.set_varchar();
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type.set_length(GET_FORMAT_MAX_LENGTH);
  format.set_calc_type(ObVarcharType);
  if (ObCharset::is_cs_nonascii(format.get_collation_type())) {
    format.set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  }
  return ret;
}

int ObExprGetFormat::calc_result2(ObObj &result,
                                   const ObObj &unit,
                                   const ObObj &format,
                                   ObExprCtx &expr_ctx) const
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntType != unit.get_type()
                  || unit.get_int() < 0 || unit.get_int() >= GET_FORMAT_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected unit date type", K(ret), K(unit));
  } else if (format.is_null()) {
    result.set_null();
  } else if (OB_UNLIKELY(ObVarcharType != format.get_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("GET_FORMAT() expected a string as format argument");
  } else {
    Format fm = FORMAT_MAX;
    ObGetFormatUnitType type = static_cast<ObGetFormatUnitType>(unit.get_int());
    const ObString str = format.get_string();
    for (int64_t i = 0; i < FORMAT_MAX; i++) {
      if (0 == str.case_compare(FORMAT_STR[i])) {
        fm = static_cast<Format>(i);
        break;
      }
    }
    if (OB_UNLIKELY(FORMAT_MAX == fm)) {
      result.set_null();
    } else {
      const char *res_str = NULL;
      if (GET_FORMAT_DATE == type) {
        result.set_varchar(DATE_FORMAT[fm]);
      } else if (GET_FORMAT_TIME == type) {
        result.set_varchar(TIME_FORMAT[fm]);
      } else if (GET_FORMAT_DATETIME == type) {
        result.set_varchar(DATETIME_FORMAT[fm]);
      }
      if (OB_UNLIKELY(ObCharset::is_cs_nonascii(result_type_.get_collation_type()))
          && OB_FAIL(convert_result_collation(result_type_, result, expr_ctx.calc_buf_))) {
        LOG_WARN("fail to convert result collation", K(ret));
      } else {
        result.set_collation(result_type_);
      }
    }
  }
  return ret;
}

int ObExprGetFormat::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get_format expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])
             || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of get_format expr is null", K(ret), K(rt_expr.args_));
  } else if (ObIntType != rt_expr.args_[0]->datum_meta_.type_
            || (ObVarcharType != rt_expr.args_[1]->datum_meta_.type_
                && ObNullType != rt_expr.args_[1]->datum_meta_.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument type", K(ret), K(rt_expr.args_[0]->datum_meta_),
            K(rt_expr.args_[1]->datum_meta_));
  } else {
    rt_expr.eval_func_ = ObExprGetFormat::calc_get_format;
  }
  return ret;
}

int ObExprGetFormat::calc_get_format(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *unit = NULL;
  ObDatum *format = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, unit, format))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (OB_UNLIKELY(unit->is_null() || unit->get_int() < 0
             || unit->get_int() >= GET_FORMAT_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected unit unit type", K(ret));
  } else if (format->is_null()) {
    expr_datum.set_null();
  } else {
    Format fm = FORMAT_MAX;
    ObGetFormatUnitType type = static_cast<ObGetFormatUnitType>(unit->get_int());
    const ObString str = format->get_string();
    for (int64_t i = 0; i < FORMAT_MAX; i++) {
      if (0 == str.case_compare(FORMAT_STR[i])) {
        fm = static_cast<Format>(i);
        break;
      }
    }
    if (OB_UNLIKELY(FORMAT_MAX == fm)) {
      expr_datum.set_null();
    } else {
      const char *res_str = NULL;
      const ObCollationType dest_cs_type = expr.datum_meta_.cs_type_;
      if (GET_FORMAT_DATE == type) {
        res_str = DATE_FORMAT[fm];
      } else if (GET_FORMAT_TIME == type) {
        res_str = TIME_FORMAT[fm];
      } else if (GET_FORMAT_DATETIME == type) {
        res_str = DATETIME_FORMAT[fm];
      }
      if (OB_LIKELY(!ObCharset::is_cs_nonascii(dest_cs_type))) {
        expr_datum.set_string(res_str);
      } else {
        ObExprStrResAlloc out_alloc(expr, ctx);
        ObString out;
        if (OB_FAIL(ObExprUtil::convert_string_collation(ObString::make_string(res_str),
                                                        CS_TYPE_UTF8MB4_GENERAL_CI,
                                                        out,
                                                        expr.datum_meta_.cs_type_,
                                                        out_alloc))) {
          LOG_WARN("convert string collation failed", K(ret));
        } else {
          expr_datum.set_string(out);
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase