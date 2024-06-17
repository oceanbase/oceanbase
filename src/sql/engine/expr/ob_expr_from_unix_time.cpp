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

#include "ob_expr_from_unix_time.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprFromUnixTime::ObExprFromUnixTime(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_FROM_UNIX_TIME,
                         N_FROM_UNIX_TIME, ONE_OR_TWO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprFromUnixTime::~ObExprFromUnixTime()
{
}

int ObExprFromUnixTime::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *params,
                                          int64_t params_count,
                                          common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
//  ObCollationType collation_connection = CS_TYPE_INVALID;
  if (OB_UNLIKELY(NULL == params ||
                  params_count <= 0 ||
                  params_count > 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument!", K(ret), K(params), K(params_count));
  } else if (1 == params_count) {
    type.set_datetime();
    ret = set_scale_for_single_param(type, params[0]);

    ObObjType param_calc_type = calc_one_param_type(params);
    params[0].set_calc_type(param_calc_type);
  } else { //params_count == 2
    type.set_varchar();
    type.set_collation_type(type_ctx.get_coll_type());
    type.set_collation_level(CS_LEVEL_COERCIBLE);
    type.set_length(DATETIME_MAX_LENGTH);

    params[0].set_calc_type(ObNumberType);
    params[1].set_calc_type(ObVarcharType);
  }
  return ret;
}

OB_INLINE int ObExprFromUnixTime::set_scale_for_single_param(ObExprResType &type,
                                                             const ObExprResType &type1) const
{
  int ret = OB_SUCCESS;
  if (type1.is_integer_type()) {
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  } else if(type1.is_varchar() || type.is_enum_or_set()) {
    // 这里是不是有bug，type1.is_enum_or_set() TODO @zongmei.zzm
    type.set_scale(MAX_SCALE_FOR_TEMPORAL);
  } else {
    type.set_scale(static_cast<ObScale>(MIN(type1.get_scale(), MAX_SCALE_FOR_TEMPORAL)));
  }
  return ret;
}

int ObExprFromUnixTime::cg_expr(ObExprCGCtx &op_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 1 && rt_expr.arg_cnt_ != 2)
      || OB_ISNULL(rt_expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (1 == rt_expr.arg_cnt_) {
    if (OB_ISNULL(rt_expr.args_[0])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null arg", K(ret));
    } else if (ObNumberType != rt_expr.args_[0]->datum_meta_.type_) {
      rt_expr.eval_func_ = &eval_one_temporal_fromtime;
    } else {
      rt_expr.eval_func_ = &eval_one_param_fromtime;
    }
  } else {
    if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null args", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]));
    } else if (0 == raw_expr.get_extra()) {
      rt_expr.eval_func_ = &eval_fromtime_normal;
    } else {
      rt_expr.eval_func_ = &eval_fromtime_special;
    }
  }
  return ret;
}

ObObjType ObExprFromUnixTime::calc_one_param_type(ObExprResType *params) const
{
  ObObjType calc_param_type = params[0].get_type();
  if (calc_param_type >= ObDateTimeType && calc_param_type <= ObYearType) {
    // do nothing
  } else {
    calc_param_type = ObNumberType;
  }
  return calc_param_type;
}

int ObExprFromUnixTime::eval_one_temporal_fromtime(const ObExpr &expr,
                                                   ObEvalCtx &ctx,
                                                   ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  expr_datum.set_null();
  return ret;
}

int ObExprFromUnixTime::eval_one_param_fromtime(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_UNLIKELY(expr.arg_cnt_ != 1)
      || OB_ISNULL(expr.args_)
      || OB_ISNULL(expr.args_[0])
      || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("failed to eval", K(ret));
  } else if (param_datum->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    int64_t usec_val;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_FAIL(get_usec_from_datum(*param_datum, alloc_guard.get_allocator(), usec_val))) {
      LOG_WARN("failed to get_usec_from_datum", K(ret));
      // if warn on failed
      ObCastMode cast_mode = CM_NONE;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                        session->is_ignore_stmt(),
                                        sql_mode, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
        expr_datum.set_null();
      }
    } else if (usec_val < 0) {
      expr_datum.set_null();
    } else if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(
                                          usec_val,
                                          tz_info,
                                          usec_val))) {
      LOG_WARN("failed to convert timestamp to datetime", K(ret));
    } else if (OB_UNLIKELY(ObTimeConverter::is_valid_datetime(usec_val))) {
      expr_datum.set_datetime(usec_val);
    } else {
      expr_datum.set_null();
    }
  }
  return ret;
}

int ObExprFromUnixTime::eval_fromtime_normal(const ObExpr &expr,
                                             ObEvalCtx &ctx,
                                             ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param1 = NULL;
  ObDatum *param2 = NULL;
  bool res_null = false;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  ObString locale_name;
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("failed to eval", K(ret));
  } else if (OB_ISNULL(param1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param1", K(ret), K(param1));
  } else if (param1->is_null()) { // mysql 模式 from_unixtime有短路逻辑
    expr_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, param2))) {
    LOG_WARN("failed to eval", K(ret));
  } else if (OB_ISNULL(param2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param2", K(ret));
  } else if (param2->is_null() || param2->get_string().empty()) { // 需要运行时cast
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    int64_t usec_val;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_FAIL(get_usec_from_datum(
                        *param1,
                        alloc_guard.get_allocator(),
                        usec_val))) {
      LOG_WARN("failed to get_usec_from_datum", K(ret));
      // warn on fail mode
      ObCastMode cast_mode = CM_NONE;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                        session->is_ignore_stmt(),
                                        sql_mode, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
      }
      expr_datum.set_null();
    } else {
      ObTime ob_time;
      ObDatum tmp_val;
      char *buf = NULL;
      int64_t pos = 0;
      const int64_t BUF_LEN = 1024;
      // 这里先用expr_datum，最后输出结果的时候会调用set_string覆盖其结果
      expr_datum.set_time(usec_val);
      if (usec_val < 0) {
        expr_datum.set_null();
      } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, BUF_LEN))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no more memory to alloc for buf", K(ret));
      } else if (OB_FAIL(ob_datum_to_ob_time_with_date(
                           expr_datum, ObTimestampType, NUMBER_SCALE_UNKNOWN_YET,
                           tz_info,
                           ob_time,
                           get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), 0, false))) {
        LOG_WARN("failed to cast datum to obtime with date", K(ret));
      } else if (OB_FAIL(session->get_locale_name(locale_name))) {
          LOG_WARN("failed to get locale time name", K(expr), K(expr_datum));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_str_format(ob_time,
                                                                param2->get_string(),
                                                                buf,
                                                                BUF_LEN,
                                                                pos,
                                                                res_null,
                                                                locale_name))) {
        LOG_WARN("failed to convert ob time to str with format");
      } else if (res_null) {
        expr_datum.set_null();
      } else {
        expr_datum.set_string(buf, static_cast<int32_t>(pos));
      }
    }
  }
  return ret;
}

int ObExprFromUnixTime::eval_fromtime_special(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param1 = NULL;
  ObDatum *param2 = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("failed to eval", K(ret));
  } else if (param1->is_null()) { // mysql 模式 from_unixtime有短路逻辑
    expr_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, param2))) {
    LOG_WARN("failed to eval", K(ret));
  } else if (param2->is_null()) {
    expr_datum.set_null();
  } else {
    // just use param as result
    expr_datum.set_string(param2->get_string());
  }
  return ret;
}

int ObExprFromUnixTime::get_usec_from_datum(const common::ObDatum &param_datum,
                                            common::ObIAllocator &alloc,
                                            int64_t &usec_val)
{
  int ret = OB_SUCCESS;
  number::ObNumber param1;
  number::ObNumber param2;
  number::ObNumber res;
  number::ObNumber param2_tmp(param_datum.get_number());
  const int64_t usecs_per_sec = USECS_PER_SEC;
  int64_t tmp = 0;
  if (OB_FAIL(param1.from(usecs_per_sec, alloc))
      || OB_FAIL(param2.from(param2_tmp, alloc))) {
    LOG_WARN("failed to get number", K(ret));
  } else if (OB_FAIL(param1.mul(param2, res, alloc))) {
    LOG_WARN("failed to mul", K(ret));
  } else if (OB_FAIL(res.extract_valid_int64_with_round(tmp))) {
    if (OB_DATA_OUT_OF_RANGE == ret) {
      ret = OB_ERR_TRUNCATED_WRONG_VALUE;
      ObString dec("DECIMAL");
      const char *num_str = param2_tmp.format();
      LOG_USER_ERROR(OB_ERR_TRUNCATED_WRONG_VALUE, dec.length(), dec.ptr(),
                     static_cast<int>(strlen(num_str)), num_str);
    }
    LOG_WARN("extract valid int64 with round failed", K(ret), K(res));
  } else {
    usec_val = tmp;
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprFromUnixTime, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(2);
  EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_SQL_MODE);
  EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_TIME_ZONE);
  return ret;
}
}
}
