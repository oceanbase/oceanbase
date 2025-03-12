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

#include "sql/engine/expr/ob_expr_timestamp_add.h"
#include "sql/engine/expr/ob_expr_mul.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprTimeStampAdd::ObExprTimeStampAdd(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TIME_STAMP_ADD, N_TIME_STAMP_ADD, 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprTimeStampAdd::~ObExprTimeStampAdd()
{
}


inline int ObExprTimeStampAdd::calc_result_type3(ObExprResType &type,
                                            ObExprResType &unit,
                                            ObExprResType &interval,
                                            ObExprResType &timestamp,
                                            common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  ObCompatibilityMode compat_mode = get_compatibility_mode();
  //not timestamp. compatible with mysql.
  type.set_varchar();
  //timestamp.set_calc_type(common::ObDateTimeType);
  type.set_length(common::ObAccuracy::MAX_ACCURACY2[compat_mode][common::ObDateTimeType].precision_
    + common::ObAccuracy::MAX_ACCURACY2[compat_mode][common::ObDateTimeType].scale_ + 1);
  type.set_collation_level(common::CS_LEVEL_IMPLICIT);
  //not connection collation. compatible with mysql.
  type.set_collation_type(common::ObCharset::get_default_collation(common::ObCharset::get_default_charset()));
  unit.set_calc_type(ObIntType);
  interval.set_calc_type(ObIntType);

  return ret;
}

void ObExprTimeStampAdd::check_reset_status(const ObCastMode cast_mode,
                                            int &ret,
                                            ObObj &result)
{
  if (OB_FAIL(ret)) {
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      ret = OB_SUCCESS;
      result.set_null();
    }
  }
}

int ObExprTimeStampAdd::calc(const int64_t unit_value,
                             ObTime &ot,
                             const int64_t ts,
                             const ObTimeConvertCtx &cvrt_ctx,
                             int64_t interval,
                             int64_t &value)
{
  int ret = OB_SUCCESS;
  static const int64_t USECS_PER_WEEK = (USECS_PER_DAY * DAYS_PER_WEEK);
  static const int64_t USECS_PER_HOUR = (static_cast<int64_t>(USECS_PER_SEC) * SECS_PER_HOUR);
  static const int64_t USECS_PER_MIN = (USECS_PER_SEC * SECS_PER_MIN);
  static const int64_t MONTH_PER_QUARTER = 3;
  int64_t quota = -1;
  int64_t delta = 0;
  switch(unit_value) {
  case DATE_UNIT_MICROSECOND: {
    quota = 1;
    //fall through
  }
  case DATE_UNIT_SECOND: {
    quota = (-1 == quota ? USECS_PER_SEC : quota);
    //fall through
  }
  case DATE_UNIT_MINUTE: {
    quota = (-1 == quota ? USECS_PER_MIN : quota);
    //fall through
  }
  case DATE_UNIT_HOUR: {
    quota = (-1 == quota ? USECS_PER_HOUR : quota);
    //fall through
  }
  case DATE_UNIT_DAY: {
    quota = (-1 == quota ? USECS_PER_DAY : quota);
    //fall through
  }
  case DATE_UNIT_WEEK: {
    quota = (-1 == quota ? USECS_PER_WEEK : quota);
    if (ObExprMul::is_mul_out_of_range(interval, quota, delta)) {
      ret = OB_DATETIME_FUNCTION_OVERFLOW;
      LOG_WARN("timestamp value is out of range", K(ret), K(quota), K(interval), K(ts), K(delta), K(value));
    } else {
      if (ObExprAdd::is_add_out_of_range(ts, delta, value)) {
        ret = OB_DATETIME_FUNCTION_OVERFLOW;
        LOG_WARN("timestamp value is out of range", K(ret), K(quota), K(interval), K(ts), K(delta), K(value));
      }
    }
    break;
  }
  case DATE_UNIT_MONTH: {
    //select timestampadd(month, 3, "2010-08-01 11:11:11");
    //diff_month = 3
    //so, result == 2010-08-01 11:11:11 + 3 month == 2010-11-01 11:11:11
    delta = interval;
    int32_t month = static_cast<int32_t>((ot.parts_[DT_YEAR]) * (MONS_PER_YEAR) + ot.parts_[DT_MON] - 1);
    if (OB_UNLIKELY(ObExprAdd::is_add_out_of_range(month, delta, month))) {
      ret = OB_DATETIME_FUNCTION_OVERFLOW;
      LOG_WARN("timestamp value is out of range", K(ret), K(ts), K(month), K(interval));
    } else {
      ot.parts_[DT_YEAR] = month / 12;
      ot.parts_[DT_MON] = month % 12 + 1;
      int32_t days = ObTimeConverter::get_days_of_month(ot.parts_[DT_YEAR], ot.parts_[DT_MON]);
      if (ot.parts_[DT_MDAY] > days) {
        ot.parts_[DT_MDAY] = days;
      }
      ot.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ot);
      if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot, cvrt_ctx, value))) {
        LOG_WARN("ob time to datetime failed", K(ret));
      }
    }
    break;
  }
  case DATE_UNIT_QUARTER: {
    //select timestampadd(quarter, -2, "2010-11-01 11:11:11");
    //diff_month = -2 * MONTH_PER_QUARTER = -2 *3 = -6
    //so, result == 2010-11-01 11:11:11 + (- 6 month) == 2010-05-01 11:11:11
    int32_t month = static_cast<int32_t>((ot.parts_[DT_YEAR]) * MONS_PER_YEAR + (ot.parts_[DT_MON] - 1));
    if (OB_UNLIKELY(ObExprMul::is_mul_out_of_range(interval, MONTH_PER_QUARTER, delta))) {
      ret = OB_DATETIME_FUNCTION_OVERFLOW;
      LOG_WARN("timestamp value is out of range", K(ret), K(ts), K(month), K(interval));
    } else if (OB_UNLIKELY(ObExprAdd::is_add_out_of_range(month, delta, month))) {
      ret = OB_DATETIME_FUNCTION_OVERFLOW;
      LOG_WARN("timestamp value is out of range", K(ret), K(ts), K(month), K(interval));
    } else {
      //IMHO, no need to define 12 as  MONTH_PER_YEAR here.
      ot.parts_[DT_YEAR] = month / 12;
      ot.parts_[DT_MON] = month % 12 + 1;
      ot.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ot);
      if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot, cvrt_ctx, value))) {
        LOG_WARN("ob time to datetime failed", K(ret));
      }
    }
    break;
  }
  case DATE_UNIT_YEAR: {
    delta = interval;
    if (OB_UNLIKELY(ObExprAdd::is_add_out_of_range(ot.parts_[DT_YEAR], delta, ot.parts_[DT_YEAR]))) {
      ret = OB_DATETIME_FUNCTION_OVERFLOW;
      LOG_WARN("timestamp value is out of range", K(ret), K(ts), K(ot.parts_[DT_YEAR]), K(interval));
    } else {
      ot.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ot);
      if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot, cvrt_ctx, value))) {
        LOG_WARN("ob time to datetime failed", K(ret));
      }
    }
    break;
  }
  default: {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(unit_value));
    break;
    }
  }
  return ret;
}

int calc_timestampadd_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObDatum *unit_datum = NULL;
  ObDatum *interval_datum = NULL;
  ObDatum *timestamp_datum = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, unit_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, interval_datum)) ||
      OB_FAIL(expr.args_[2]->eval(ctx, timestamp_datum))) {
    LOG_WARN("eval arg failed", K(ret), KP(unit_datum), KP(interval_datum),
              KP(timestamp_datum));
  } else if (unit_datum->is_null() || interval_datum->is_null() ||
             timestamp_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    int64_t ts = 0;
    int64_t interval_int = interval_datum->get_int();
    int64_t res = 0;
    ObTime ot;
    ObTimeConvertCtx cvrt_ctx(tz_info, false);
    char *buf = NULL;
    int64_t buf_len = OB_CAST_TO_VARCHAR_MAX_LENGTH;
    int64_t out_len = 0;
    if (OB_FAIL(ob_datum_to_ob_time_with_date(*timestamp_datum,
                expr.args_[2]->datum_meta_.type_,
                expr.args_[2]->datum_meta_.scale_,
                cvrt_ctx.tz_info_, ot,
                get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), 0,
                expr.args_[2]->obj_meta_.has_lob_header()))) {
      LOG_WARN("cast to ob time failed", K(ret), K(*timestamp_datum));
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot, cvrt_ctx, ts))) {
      LOG_WARN("ob time to datetime failed", K(ret));
    } else if (OB_FAIL(ObExprTimeStampAdd::calc(unit_datum->get_int(), ot, ts, cvrt_ctx,
                            interval_int, res))) {
      LOG_WARN("calc failed", K(ret), K(*unit_datum), K(ts), K(interval_int));
    } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(buf_len));
    } else if (OB_FAIL(common_datetime_string(expr, ObDateTimeType, ObVarcharType,
                                              expr.args_[2]->datum_meta_.scale_, false,
                                              res, ctx, buf, buf_len, out_len))) {
      LOG_WARN("common_datetime_string failed", K(ret), K(res), K(expr));
    } else {
      res_datum.set_string(ObString(out_len, buf));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(session)) {
    ObCastMode cast_mode = CM_NONE;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode,
                                      cast_mode);
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      ret = OB_SUCCESS;
    }
    res_datum.set_null();
  }
  return ret;
}

int ObExprTimeStampAdd::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_timestampadd_expr;
  // The vectorization of other types for the expression not completed yet.
  if (ob_is_date_or_mysql_date(rt_expr.args_[2]->datum_meta_.type_)
      || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[2]->datum_meta_.type_)) {
    rt_expr.eval_vector_func_ = calc_timestamp_add_vector;
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprTimeStampAdd, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(2);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_TIME_ZONE);
  return ret;
}


template <typename ArgVec, typename UnitVec, typename IntervalVec, typename ResVec, typename IN_TYPE>
int vector_timestamp_add(
    const ObExpr &expr, ObEvalCtx &ctx,
    const ObBitVector &skip, const EvalBound &bound,  ObSQLMode sql_mode)
{
  int ret = OB_SUCCESS;
  UnitVec *unit_type_vec = static_cast<UnitVec *>(expr.args_[0]->get_vector(ctx));
  IntervalVec *interval_vec = static_cast<IntervalVec *>(expr.args_[1]->get_vector(ctx));
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[2]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  const ObObjType arg_type = expr.args_[2]->datum_meta_.type_;
  ObObjTypeClass arg_tc = ob_obj_type_class(arg_type);
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  const common::ObTimeZoneInfo *tz_info = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, session);
  int64_t tmp = 0;  // unused
  int64_t tz_offset = 0;
  ObString nls_format;
  if (lib::is_oracle_mode() && OB_FAIL(common_get_nls_format(
                                    session, ctx, &expr, ObDateTimeType, false, nls_format))) {
      LOG_WARN("common_get_nls_format failed", K(ret));
  } else if (eval_flags.accumulate_bit_cnt(bound) == bound.range_size()) {
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (OB_FAIL(get_tz_offset(tz_info, tz_offset))) {
    LOG_WARN("failed to get offset between utc and local", K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(tz_info, false);
    tz_offset = (ObTimestampType == arg_type) ? tz_offset : 0;
    DateType date = 0;
    UsecType usec = 0;
    ObScale in_scale = expr.args_[2]->datum_meta_.scale_;
    ObCastMode cast_mode = CM_NONE;
    ObSQLUtils::get_default_cast_mode(
        session->get_stmt_type(), session->is_ignore_stmt(), sql_mode, cast_mode);
    ObTime ot;

    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx) || unit_type_vec->is_null(idx) || interval_vec->is_null(idx)) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
        continue;
      }
      char *buf = NULL;
      int64_t buf_len = OB_CAST_TO_VARCHAR_MAX_LENGTH;
      int64_t out_len = 0;
      int64_t ts = 0;
      IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
      ObDateSqlMode date_sql_mode;
      if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
        if (OB_FAIL(ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ot))) {
          LOG_WARN("parse_ob_time fail", K(ret));
        } else {
          ot.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ot);
          if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot, cvrt_ctx, ts))) {
            LOG_WARN("ob_time_to_datetime fail", K(ret));
          }
        }
      } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, oceanbase::lib::is_oracle_mode(), date, usec))) {
        LOG_WARN("get_date_usec_from_vec failed", K(ret), K(date), K(usec), K(tz_offset));
      } else if (OB_FAIL(ObTimeConverter::date_to_ob_time(date, ot))) {
        LOG_WARN("failed to convert date part to obtime", K(ret), K(date));
      } else if (OB_FAIL(ObTimeConverter::time_to_ob_time(usec, ot))) {
        LOG_WARN("failed to convert time part to obtime", K(ret), K(usec));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot, cvrt_ctx, ts))) {
        LOG_WARN("ob time to datetime failed", K(ret));
      }
      if (OB_SUCC(ret)) {
        int64_t res = 0;
        const ObTimeZoneInfo *null_tz_info = NULL;  // refer to calc_timestampadd_expr()
        if (OB_FAIL(ObExprTimeStampAdd::calc(unit_type_vec->get_int(idx), ot, ts, cvrt_ctx,
                                              interval_vec->get_int(idx), res))) {
          LOG_WARN("calculate ts after adding interval failed", K(ret), K(ts));
        } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, buf_len, idx))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(buf_len));
        } else if(OB_FAIL(ObTimeConverter::datetime_to_str(res, null_tz_info, nls_format, in_scale,
                                                            buf, buf_len, out_len))) {
          LOG_WARN("failed to convert datetime to string",
                    K(ret), K(res), K(nls_format), K(in_scale), K(buf), K(out_len));
        } else {
          res_vec->set_string(idx, ObString(out_len, buf));
          eval_flags.set(idx);
        }
      }
      if (OB_FAIL(ret)) {
        if(CM_IS_WARN_ON_FAIL(cast_mode)) {
          ret = OB_SUCCESS;
        }
        res_vec->set_null(idx);
        eval_flags.set(idx);
      }
    }
  }
  return ret;
}

#define DEF_TIMESTAMP_ADD_VECTOR(arg_type, res_type, IN_TYPE)\
  if (VEC_FIXED == unit_format && VEC_FIXED == interval_format) {\
    ret = vector_timestamp_add<arg_type, IntegerFixedVec, IntegerFixedVec, res_type, IN_TYPE>\
              (expr, ctx, skip, bound, sql_mode);\
  } else if (VEC_FIXED == unit_format && VEC_UNIFORM == interval_format) {\
    ret = vector_timestamp_add<arg_type, IntegerFixedVec, IntegerUniVec, res_type, IN_TYPE>\
              (expr, ctx, skip, bound, sql_mode);\
  } else if (VEC_FIXED == unit_format && VEC_UNIFORM_CONST == interval_format) {\
    ret = vector_timestamp_add<arg_type, IntegerFixedVec, IntegerUniCVec, res_type, IN_TYPE>\
              (expr, ctx, skip, bound, sql_mode);\
  } else if (VEC_UNIFORM == unit_format && VEC_UNIFORM == interval_format) {\
    ret = vector_timestamp_add<arg_type, IntegerUniVec, IntegerUniVec, res_type, IN_TYPE>\
              (expr, ctx, skip, bound, sql_mode);\
  } else if (VEC_UNIFORM == unit_format && VEC_UNIFORM_CONST == interval_format) {\
    ret = vector_timestamp_add<arg_type, IntegerUniVec, IntegerUniCVec, res_type, IN_TYPE>\
              (expr, ctx, skip, bound, sql_mode);\
  } else if (VEC_UNIFORM == unit_format && VEC_FIXED == interval_format) {\
    ret = vector_timestamp_add<arg_type, IntegerUniVec, IntegerFixedVec, res_type, IN_TYPE>\
              (expr, ctx, skip, bound, sql_mode);\
  } else if (VEC_UNIFORM_CONST == unit_format && VEC_UNIFORM_CONST == interval_format) {\
    ret = vector_timestamp_add<arg_type, IntegerUniCVec, IntegerUniCVec, res_type, IN_TYPE>\
              (expr, ctx, skip, bound, sql_mode);\
  } else if (VEC_UNIFORM_CONST == unit_format && VEC_UNIFORM == interval_format) {\
    ret = vector_timestamp_add<arg_type, IntegerUniCVec, IntegerUniVec, res_type, IN_TYPE>\
              (expr, ctx, skip, bound, sql_mode);\
  } else if (VEC_UNIFORM_CONST == unit_format && VEC_FIXED == interval_format) {\
    ret = vector_timestamp_add<arg_type, IntegerUniCVec, IntegerFixedVec, res_type, IN_TYPE>\
              (expr, ctx, skip, bound, sql_mode);\
  } else {\
    ret = vector_timestamp_add<ObVectorBase, ObVectorBase, ObVectorBase, ObVectorBase, IN_TYPE>\
              (expr, ctx, skip, bound, sql_mode);\
  }

#define DISPATCH_TIMESTAMP_ADD_IN_TYPE(TYPE)\
  if (VEC_FIXED == arg_format && VEC_DISCRETE == res_format) {\
    DEF_TIMESTAMP_ADD_VECTOR(CONCAT(TYPE,FixedVec), StrDiscVec, CONCAT(TYPE,Type));\
  } else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {\
    DEF_TIMESTAMP_ADD_VECTOR(CONCAT(TYPE,FixedVec), StrUniVec, CONCAT(TYPE,Type));\
  } else if (VEC_FIXED == arg_format && VEC_CONTINUOUS == res_format) {\
    DEF_TIMESTAMP_ADD_VECTOR(CONCAT(TYPE,FixedVec), StrContVec, CONCAT(TYPE,Type));\
  } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {\
    DEF_TIMESTAMP_ADD_VECTOR(CONCAT(TYPE,UniVec), StrDiscVec, CONCAT(TYPE,Type));\
  } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
    DEF_TIMESTAMP_ADD_VECTOR(CONCAT(TYPE,UniVec), StrUniVec, CONCAT(TYPE,Type));\
  } else if (VEC_UNIFORM == arg_format && VEC_CONTINUOUS == res_format) {\
    DEF_TIMESTAMP_ADD_VECTOR(CONCAT(TYPE,UniVec), StrContVec, CONCAT(TYPE,Type));\
  } else {\
    ret = vector_timestamp_add<ObVectorBase, ObVectorBase, ObVectorBase, ObVectorBase, CONCAT(TYPE,Type)>(expr, ctx, skip, bound, sql_mode);\
  }

int ObExprTimeStampAdd::calc_timestamp_add_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObSQLMode sql_mode = 0;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))
          || OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))
          || OB_FAIL(expr.args_[2]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    VectorFormat unit_format = expr.args_[0]->get_format(ctx);
    VectorFormat interval_format = expr.args_[1]->get_format(ctx);
    VectorFormat arg_format = expr.args_[2]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObObjTypeClass arg_tc = ob_obj_type_class(expr.args_[2]->datum_meta_.type_);

    if (ObMySQLDateTC == arg_tc) {
      DISPATCH_TIMESTAMP_ADD_IN_TYPE(MySQLDate);
    } else if (ObMySQLDateTimeTC == arg_tc) {
      DISPATCH_TIMESTAMP_ADD_IN_TYPE(MySQLDateTime);
    } else if (ObDateTC == arg_tc) {
      DISPATCH_TIMESTAMP_ADD_IN_TYPE(Date);
    } else if (ObDateTimeTC == arg_tc) {
      DISPATCH_TIMESTAMP_ADD_IN_TYPE(DateTime);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

#undef DISPATCH_TIMESTAMP_ADD_IN_TYPE
#undef DEF_TIMESTAMP_ADD_VECTOR

} //namespace sql
} //namespace oceanbase
