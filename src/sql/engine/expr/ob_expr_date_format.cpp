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

}  // namespace sql
}  // namespace oceanbase
