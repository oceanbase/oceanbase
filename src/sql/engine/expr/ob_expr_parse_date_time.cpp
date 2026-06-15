/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_parse_date_time.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "share/datum/ob_datum.h"
#include "share/vector/ob_vector_define.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

namespace
{

void print_parse_date_time_user_warning(const int ret, ObString date_str, ObString func_name,
                                        ObString format_str = "")
{
  if (OB_INVALID_DATE_FORMAT == ret) {
    ObString date_type_str("date");
    LOG_USER_WARN(OB_ERR_TRUNCATED_WRONG_VALUE, date_type_str.length(), date_type_str.ptr(),
                  date_str.length(), date_str.ptr());
  } else if (OB_INVALID_DATE_VALUE == ret || OB_INVALID_ARGUMENT == ret) {
    ObString datetime_type_str("datetime");
    LOG_USER_WARN(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                  datetime_type_str.length(), datetime_type_str.ptr(),
                  date_str.length(), date_str.ptr(),
                  func_name.length(), func_name.ptr());
  } else if (OB_INVALID_DATE_FORMAT_END == ret) {
    ObString date_type_str("format");
    LOG_USER_WARN(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                  date_type_str.length(), date_type_str.ptr(),
                  format_str.length(), format_str.ptr(),
                  func_name.length(), func_name.ptr());
  }
}

int set_parse_date_time_error_code(const int ori_ret, ObString date_str, ObString func_name)
{
  int ret = ori_ret;
  if (OB_INVALID_DATE_FORMAT == ret) {
    ret = OB_ERR_TRUNCATED_WRONG_VALUE;
    ObString date_type_str("date");
    LOG_USER_ERROR(OB_ERR_TRUNCATED_WRONG_VALUE, date_type_str.length(), date_type_str.ptr(),
                   date_str.length(), date_str.ptr());
  } else if (OB_INVALID_DATE_VALUE == ret || OB_INVALID_ARGUMENT == ret) {
    ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
    ObString datetime_type_str("datetime");
    LOG_USER_ERROR(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                   datetime_type_str.length(), datetime_type_str.ptr(),
                   date_str.length(), date_str.ptr(),
                   func_name.length(), func_name.ptr());
  }
  return ret;
}

}

ObExprParseDateTime::ObExprParseDateTime(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PARSE_DATE_TIME, N_PARSE_DATE_TIME, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprParseDateTime::~ObExprParseDateTime()
{
}


int ObExprParseDateTime::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types,
                                          int64_t param_num,
                                          common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  ObObjType res_type = type_ctx.enable_mysql_compatible_dates() ?
      ObMySQLDateTimeType : ObDateTimeType;
  if (OB_UNLIKELY(param_num < 1 || param_num > 2)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("param number of parse_datetime at least 1 and at most 2", K(ret), K(param_num));
  } else {
    // Check each parameter type validity
    for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (!types[i].is_null() && !ob_is_string_type(types[i].get_type()) &&
          !ob_is_enumset_tc(types[i].get_type()) && !ob_is_null(types[i].get_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("the parameter is not valid for parse_datetime", K(ret), K(i), K(types[i].get_type()));
      }
    }

    if (OB_SUCC(ret)) {
      ObExprResType &date = types[0];
      ObExprResType *format = (param_num >= 2) ? &types[1] : nullptr;
      if (param_num >= 2 && OB_NOT_NULL(format) && !format->is_null()) {
        ObObj format_obj = format->get_param();
        if (OB_UNLIKELY(format_obj.is_null())) {
          type.set_type(res_type);
          type.set_scale(MAX_SCALE_FOR_TEMPORAL);
          type.set_precision(DATETIME_MIN_LENGTH + MAX_SCALE_FOR_TEMPORAL);
        } else if (!format_obj.is_string_type()) {
          type.set_type(res_type);
          type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
          type.set_precision(DATETIME_MIN_LENGTH + DEFAULT_SCALE_FOR_INTEGER);
        } else {
          ObString format_str = format_obj.get_varchar();
          uint32_t pos = 0;
          pos = ObCharset::locate(format_obj.get_collation_type(),
                                  format_str.ptr(), format_str.length(), "%f", 2, 1);

          if (0 == pos) {
            type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
          } else {
            type.set_scale(MAX_SCALE_FOR_TEMPORAL);
          }

          type.set_type(res_type);
          type.set_precision(static_cast<ObPrecision>(DATETIME_MIN_LENGTH + type.get_scale()));
        }
      } else {
        // Default precision and scale when no format or format is null (default format has no %f)
        type.set_type(res_type);
        type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
        type.set_precision(DATETIME_MIN_LENGTH + DEFAULT_SCALE_FOR_INTEGER);
      }

      // Set calc types
      date.set_calc_type(ObVarcharType);
      if (OB_NOT_NULL(format)) {
        format->set_calc_type(ObVarcharType);
      }
    }
  }
  return ret;
}


// If expr result type is ObMySQLDateType or ObMySQLDateTimeType, the value of `res_int` is
// ObMySQLDateTime, otherwise it is datetime value.
int ObExprParseDateTime::calc(const ObExpr &expr, ObEvalCtx &ctx, bool &is_null, int64_t &res_int)
{
  int ret = OB_SUCCESS;
  is_null = false;
  res_int = 0;
  ObDatum *date_datum = NULL;
  ObDatum *fmt_datum = NULL;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSolidifiedVarsGetter helper(expr, ctx, session);
  ObSQLMode sql_mode = 0;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, date_datum))) {
    LOG_WARN("eval date arg failed", K(ret), K(expr));
  } else if (expr.arg_cnt_ >= 2 && OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
    LOG_WARN("eval format arg failed", K(ret), K(expr));
  } else if (date_datum->is_null() ||
             (expr.arg_cnt_ >= 2 && fmt_datum->is_null())) {
    is_null = true;
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else {
    const bool is_mysql_datetime = ob_is_mysql_compact_dates_type(expr.datum_meta_.type_);
    const ObString &date_str = date_datum->get_string();
    const ObString fmt_str = expr.arg_cnt_ >= 2 ? fmt_datum->get_string() : ObString("%Y-%m-%d %H:%i:%s");

    ObTimeConvertCtx cvrt_ctx(TZ_INFO(session), false);
    ObDateSqlMode date_sql_mode;
    date_sql_mode.no_zero_in_date_ = is_no_zero_in_date(sql_mode);
    date_sql_mode.allow_incomplete_dates_ = !is_no_zero_in_date(sql_mode);
    date_sql_mode.allow_invalid_dates_ = is_allow_invalid_dates(sql_mode);
    if (is_mysql_datetime) {
      ObMySQLDateTime res_mdt = 0;
      ObTime ob_time(DT_TYPE_MYSQL_DATETIME);
      if (OB_FAIL(ObTimeConverter::str_to_ob_time_format</*is_clickhouse_style=*/true>(date_str, fmt_str, ob_time, NULL, date_sql_mode))) {
        LOG_WARN("failed to convert string to ob_time with format", K(ret), K(date_sql_mode));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_mdatetime(ob_time, res_mdt))) {
        LOG_WARN("failed to convert ob_time to mdatetime", K(ret));
      }
      res_int = res_mdt.datetime_;
    } else {
      ObTime ob_time(DT_TYPE_DATETIME);
      ObDateSqlMode local_date_sql_mode = date_sql_mode;
      if (cvrt_ctx.is_timestamp_) {
        local_date_sql_mode.allow_invalid_dates_ = false;
      }
      if (OB_FAIL(ObTimeConverter::str_to_ob_time_format</*is_clickhouse_style=*/true>(date_str, fmt_str, ob_time, NULL, local_date_sql_mode))) {
        LOG_WARN("failed to convert string to datetime", K(ret));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, res_int))) {
        LOG_WARN("failed to convert datetime to seconds", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = ret;
      ObCastMode def_cast_mode = CM_NONE;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                        session->is_ignore_stmt(),
                                        sql_mode,
                                        def_cast_mode);
      if (CM_IS_WARN_ON_FAIL(def_cast_mode)) {
        if (OB_INVALID_DATE_FORMAT == tmp_ret || OB_INVALID_DATE_VALUE == tmp_ret
            || OB_INVALID_ARGUMENT == tmp_ret || OB_INVALID_DATE_FORMAT_END == tmp_ret) {
          ret = OB_SUCCESS;
          is_null = true;
          print_parse_date_time_user_warning(tmp_ret, date_str, ObString(N_PARSE_DATE_TIME), fmt_str);
        } else {
          ret = tmp_ret;
        }
      } else {
        ret = set_parse_date_time_error_code(tmp_ret, date_str, ObString(N_PARSE_DATE_TIME));
      }
    }
  }

  return ret;
}


int ObExprParseDateTime::calc_parse_date_time(const ObExpr &expr, ObEvalCtx &ctx,
                                                        ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  bool is_null = false;
  int64_t datetime_int = 0;
  if (OB_FAIL(calc(expr, ctx, is_null, datetime_int))) {
    LOG_WARN("calc parse_date_time failed", K(ret), K(expr));
  } else if (is_null) {
    res_datum.set_null();
  } else {
    res_datum.set_datetime(datetime_int);
  }
  return ret;
}

template <typename StrVec, typename FmtVec, typename ResVec>
int ObExprParseDateTime::vector_parse_date_time(const ObExpr &expr, ObEvalCtx &ctx,
                                               const ObBitVector &skip, const EvalBound &bound) {
  int ret = OB_SUCCESS;
  StrVec *str_vec = static_cast<StrVec *>(expr.args_[0]->get_vector(ctx));
  FmtVec *fmt_vec = expr.arg_cnt_ >= 2 ? static_cast<FmtVec *>(expr.args_[1]->get_vector(ctx)) : nullptr;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSolidifiedVarsGetter helper(expr, ctx, session);
  ObSQLMode sql_mode = 0;
  constexpr bool is_fmt_const = std::is_same<FmtVec, ConstUniformFormat>::value;
  ObString fmt_str = ObString("%Y-%m-%d %H:%i:%s");

  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else {
    const bool is_mysql_datetime = ob_is_mysql_compact_dates_type(expr.datum_meta_.type_);
    ObTimeConvertCtx cvrt_ctx(TZ_INFO(session), false);
    ObDateSqlMode date_sql_mode;
    date_sql_mode.no_zero_in_date_ = is_no_zero_in_date(sql_mode);
    date_sql_mode.allow_incomplete_dates_ = !is_no_zero_in_date(sql_mode);
    date_sql_mode.allow_invalid_dates_ = is_allow_invalid_dates(sql_mode);

    if (OB_NOT_NULL(fmt_vec) && is_fmt_const) {
      fmt_str = fmt_vec->get_string(0);
    }
    for (int64_t i = bound.start(); i < bound.end() && OB_SUCC(ret); i++) {
      if (!(skip.at(i) || eval_flags.at(i))) {
        if (str_vec->is_null(i) || (OB_NOT_NULL(fmt_vec) && fmt_vec->is_null(i))) {
          res_vec->set_null(i);
        } else {
          const ObString &date_str = str_vec->get_string(i);
          if (!is_fmt_const && OB_NOT_NULL(fmt_vec)) {
            fmt_str = fmt_vec->get_string(i);
          }

          int64_t res_int = 0;
          if (is_mysql_datetime) {
            ObMySQLDateTime res_mdt = 0;
            ObTime ob_time(DT_TYPE_MYSQL_DATETIME);
            if (OB_FAIL(ObTimeConverter::str_to_ob_time_format</*is_clickhouse_style=*/true>(date_str, fmt_str, ob_time, NULL, date_sql_mode))) {
              LOG_WARN("failed to convert string to ob_time with format", K(ret), K(date_sql_mode));
            } else if (OB_FAIL(ObTimeConverter::ob_time_to_mdatetime(ob_time, res_mdt))) {
              LOG_WARN("failed to convert ob_time to mdatetime", K(ret));
            } else {
              res_int = res_mdt.datetime_;
              res_vec->set_datetime(i, res_int);
            }
          } else {
            ObTime ob_time(DT_TYPE_DATETIME);
            ObDateSqlMode local_date_sql_mode = date_sql_mode;
            if (cvrt_ctx.is_timestamp_) {
              local_date_sql_mode.allow_invalid_dates_ = false;
            }
            if (OB_FAIL(ObTimeConverter::str_to_ob_time_format</*is_clickhouse_style=*/true>(date_str, fmt_str, ob_time, NULL, local_date_sql_mode))) {
              LOG_WARN("failed to convert string to datetime", K(ret));
            } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, res_int))) {
              LOG_WARN("failed to convert datetime to seconds", K(ret));
            } else {
              res_vec->set_datetime(i, res_int);
            }
          }
          if (OB_FAIL(ret)) {
            int tmp_ret = ret;
            ObCastMode def_cast_mode = CM_NONE;
            ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                              session->is_ignore_stmt(),
                                              sql_mode,
                                              def_cast_mode);
            if (CM_IS_WARN_ON_FAIL(def_cast_mode)) {
              if (OB_INVALID_DATE_FORMAT == tmp_ret || OB_INVALID_DATE_VALUE == tmp_ret
                  || OB_INVALID_ARGUMENT == tmp_ret || OB_INVALID_DATE_FORMAT_END == tmp_ret) {
                ret = OB_SUCCESS;
                res_vec->set_null(i);
                print_parse_date_time_user_warning(tmp_ret, date_str, ObString(N_PARSE_DATE_TIME), fmt_str);
              } else {
                ret = tmp_ret;
              }
            } else {
              ret = set_parse_date_time_error_code(tmp_ret, date_str, ObString(N_PARSE_DATE_TIME));
            }
          }
        }
        if (OB_SUCC(ret)) {
          eval_flags.set(i);
        }
      }
    }
  }
  return ret;
}

int ObExprParseDateTime::calc_parse_date_time_vector(VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval parse_date_time date arg", K(ret));
  } else if (expr.arg_cnt_ >= 2 && OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval parse_date_time format arg", K(ret));
  } else {
    VectorFormat str_format = expr.args_[0]->get_format(ctx);
    VectorFormat fmt_format = expr.arg_cnt_ >= 2 ? expr.args_[1]->get_format(ctx) : VEC_UNIFORM_CONST;
    VectorFormat res_format = expr.get_format(ctx);
    bool fmt_is_const = expr.arg_cnt_ >= 2 ? !expr.args_[1]->is_batch_result() : true;

    if (fmt_is_const) {
      if (VEC_DISCRETE == str_format && VEC_FIXED == res_format) {
        ret = vector_parse_date_time<StrDiscVec, ConstUniformFormat, DateTimeFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == str_format && VEC_FIXED == res_format) {
        ret = vector_parse_date_time<StrUniVec, ConstUniformFormat, DateTimeFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == str_format && VEC_FIXED == res_format) {
        ret = vector_parse_date_time<StrContVec, ConstUniformFormat, DateTimeFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == str_format && VEC_UNIFORM == res_format) {
        ret = vector_parse_date_time<StrDiscVec, ConstUniformFormat, DateTimeUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == str_format && VEC_UNIFORM == res_format) {
        ret = vector_parse_date_time<StrUniVec, ConstUniformFormat, DateTimeUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == str_format && VEC_UNIFORM == res_format) {
        ret = vector_parse_date_time<StrContVec, ConstUniformFormat, DateTimeUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else {
        ret = vector_parse_date_time<ObVectorBase, ConstUniformFormat, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
      }
    } else {
      if (VEC_DISCRETE == str_format && VEC_FIXED == res_format) {
        ret = vector_parse_date_time<StrDiscVec, ObVectorBase, DateTimeFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == str_format && VEC_FIXED == res_format) {
        ret = vector_parse_date_time<StrUniVec, ObVectorBase, DateTimeFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == str_format && VEC_FIXED == res_format) {
        ret = vector_parse_date_time<StrContVec, ObVectorBase, DateTimeFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_DISCRETE == str_format && VEC_UNIFORM == res_format) {
        ret = vector_parse_date_time<StrDiscVec, ObVectorBase, DateTimeUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_UNIFORM == str_format && VEC_UNIFORM == res_format) {
        ret = vector_parse_date_time<StrUniVec, ObVectorBase, DateTimeUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else if (VEC_CONTINUOUS == str_format && VEC_UNIFORM == res_format) {
        ret = vector_parse_date_time<StrContVec, ObVectorBase, DateTimeUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
      } else {
        ret = vector_parse_date_time<ObVectorBase, ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
      }
    }
  }
  return ret;
}

int ObExprParseDateTime::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (ob_is_datetime_or_mysql_datetime(rt_expr.datum_meta_.type_)) {
    rt_expr.eval_func_ = ObExprParseDateTime::calc_parse_date_time;
    rt_expr.eval_vector_func_ = ObExprParseDateTime::calc_parse_date_time_vector;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected res type, only datetime type is supported", K(ret), K(rt_expr.datum_meta_.type_));
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprParseDateTime, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  return ret;
}

}
}
