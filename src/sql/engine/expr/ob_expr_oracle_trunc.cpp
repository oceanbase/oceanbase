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

#include "ob_expr_oracle_trunc.h"
#include "sql/engine/expr/ob_expr_truncate.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/ob_name_def.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"

namespace oceanbase {
using namespace common;
namespace sql {
#define GET_SCALE_FOR_CALC(scale) \
  (scale < 0 ? max(number::ObNumber::MIN_SCALE, scale) : min(number::ObNumber::MAX_SCALE, scale))

ObExprOracleTrunc::ObExprOracleTrunc(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ORA_TRUNC, N_ORA_TRUNC, ONE_OR_TWO, NOT_ROW_DIMENSION)
{}

ObExprOracleTrunc::ObExprOracleTrunc(ObIAllocator& alloc, const char* name)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ORA_TRUNC, name, ONE_OR_TWO, NOT_ROW_DIMENSION)
{}

ObExprTrunc::ObExprTrunc(ObIAllocator& alloc) : ObExprOracleTrunc(alloc, N_TRUNC)
{}

int ObExprOracleTrunc::calc_result1(ObObj& result, const ObObj& input, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (input.is_null()) {
    result.set_null();
  } else if (input.is_number()) {
    ObObj default_format;
    number::ObNumber zero_num;
    zero_num.set_zero();
    default_format.set_number(zero_num);
    ret = calc_with_decimal(result, input, default_format, expr_ctx);
  } else if (input.is_datetime()) {
    ObObj default_format;
    default_format.set_varchar("DD");
    ret = calc_with_date(result, input, default_format, expr_ctx);
  } else if (input.is_float()) {
    result.set_float(truncf(input.get_float()));
  } else if (input.is_double()) {
    result.set_double(trunc(input.get_double()));
  } else {
    ret = OB_INVALID_NUMERIC;
    LOG_WARN("invalid numeric. number is expected", K(ret), K(input));
  }
  return ret;
}

int ObExprOracleTrunc::calc_result2(ObObj& result, const ObObj& input, const ObObj& param, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (input.is_null() || param.is_null()) {
    result.set_null();
  } else if (input.is_datetime()) {
    ret = calc_with_date(result, input, param, expr_ctx);
  } else if (input.is_number()) {
    ret = calc_with_decimal(result, input, param, expr_ctx);
  } else {
    ret = OB_INVALID_NUMERIC;
    LOG_WARN("Invalid numeric. trunc expected number or date", K(ret), K(input), K(param));
  }
  return ret;
}

int ObExprOracleTrunc::calc_resultN(ObObj& result, const ObObj* params, int64_t params_count, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == params || params_count <= 0 || params_count > 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(params), K(params_count));
  } else if (1 == params_count) {
    ret = calc_result1(result, params[0], expr_ctx);
  } else if (2 == params_count) {
    ret = calc_result2(result, params[0], params[1], expr_ctx);
  } else {
    ret = OB_INVALID_NUMERIC;
    LOG_WARN("Invalid numeric. trunc expected number or date", K(ret), K(params[0]), K(params_count));
  }
  return ret;
}

int ObExprOracleTrunc::calc_with_date(
    ObObj& result, const ObObj& source, const ObObj& format, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (OB_UNLIKELY(ObDateTimeTC != source.get_type_class())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(source), K(format));
  } else if (OB_UNLIKELY(ObStringTC != format.get_type_class())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("inconsistent datatypes", K(ret), K(format));
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(source.get_type()), ob_obj_type_str(format.get_type()));
  } else if (OB_FAIL(ob_obj_to_ob_time_with_date(source, get_timezone_info(expr_ctx.my_session_), ob_time))) {
    LOG_WARN("failed to convert obj to ob time", K(ret), K(source), K(format));
  } else {
    LOG_DEBUG("succ to get ob_time", K(ob_time), K(source));
    int64_t dt = source.get_datetime();
    ObTimeConvertCtx cvrt_ctx(TZ_INFO(expr_ctx.my_session_), false);
    if (OB_FAIL(ObExprTRDateFormat::trunc_new_obtime(ob_time, format.get_string()))) {
      LOG_WARN("fail to calc datetime", K(ret), K(source), K(format));
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, dt))) {
      LOG_WARN("fail to cast ob_time to datetime", K(ret), K(source), K(format));
    } else {
      result.set_datetime(dt);
    }
  }
  return ret;
}

int ObExprOracleTrunc::calc_with_decimal(
    ObObj& result, const ObObj& source, const ObObj& format, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!source.is_number()) || OB_UNLIKELY(!format.is_number()) || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(source), K(format));
  } else {
    number::ObNumber nmb;
    int64_t scale = 0;
    if (OB_FAIL(format.get_number().extract_valid_int64_with_trunc(scale))) {
      LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(format.get_number()));
    } else if (OB_FAIL(nmb.from(source.get_number(), *(expr_ctx.calc_buf_)))) {
      LOG_WARN("deep copy failed", K(ret), K(source.get_number()));
    } else if (OB_FAIL(nmb.trunc(GET_SCALE_FOR_CALC(scale)))) {
      LOG_WARN("round number failed", K(ret), K(source), K(scale), K(format));
    } else if (OB_FAIL(ObExprTruncate::set_trunc_val(result, nmb, expr_ctx, result_type_.get_type()))) {
      LOG_WARN("set trunc val failed", K(ret), K(nmb), K(result_type_));
    }
  }
  return ret;
}

int ObExprOracleTrunc::calc_result_typeN(
    ObExprResType& type, ObExprResType* params, int64_t params_count, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  const ObSQLSessionInfo* session = dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast basic session to sql session failed", K(ret));
  } else if (OB_UNLIKELY(NULL == params) || OB_UNLIKELY(params_count <= 0) || OB_UNLIKELY(params_count > 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(params), K(params_count));
  } else {
    ObObjType result_type = ObMaxType;
    if (OB_FAIL(ObExprResultTypeUtil::get_round_result_type(result_type, params[0].get_type()))) {
      LOG_WARN("fail to get_round_result_type", K(ret), K(params[0].get_type()));
    } else {
      if (!lib::is_oracle_mode() && ObDateTimeTC == params[0].get_type_class()) {
        // for mysql mode
        result_type = ObDateTimeType;
      }
      if (ObDateTimeType == result_type) {
        type.set_scale(DEFAULT_SCALE_FOR_DATE);
        params[0].set_calc_type(result_type);
        type.set_type(result_type);
      } else {
        params[0].set_calc_type(result_type);
        type.set_type(result_type);
        type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
        type.set_precision(PRECISION_UNKNOWN_YET);
        type.set_calc_scale(0);
        if (params_count > 1) {
          // compatible with Oracle. when got two param, result type is Number.
          // otherwise result type is decided by arg type.
          params[0].set_calc_type(ObNumberType);
          params[1].set_calc_type(ObNumberType);
          type.set_type(ObNumberType);
        }
      }
    }
  }
  return ret;
}

int calc_trunc_expr_datetime(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // trunc(x, d)
  ObDatum* x_datum = NULL;
  ObDatum* d_datum = NULL;
  const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
  const ObObjType res_type = expr.datum_meta_.type_;
  if (OB_UNLIKELY(1 != expr.arg_cnt_ && 2 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_UNLIKELY(arg_type != res_type || ObDateTimeType != arg_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg_type of res_type", K(ret), K(arg_type), K(res_type));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (x_datum->is_null()) {
    res_datum.set_null();
  } else if (2 == expr.arg_cnt_ && OB_FAIL(expr.args_[1]->eval(ctx, d_datum))) {
    LOG_WARN("eval arg 1 failed", K(ret));
  } else if (2 == expr.arg_cnt_ && d_datum->is_null()) {
    res_datum.set_null();
  } else {
    ObTime ob_time;
    ObString fmt_str("DD");
    if (2 == expr.arg_cnt_) {
      const ObObjType fmt_type = expr.args_[1]->datum_meta_.type_;
      if (OB_UNLIKELY(ObStringTC != ob_obj_type_class(fmt_type))) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("inconsistent datatypes", K(ret), K(fmt_type));
        LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(arg_type), ob_obj_type_str(fmt_type));
      } else {
        fmt_str = d_datum->get_string();
      }
    }
    ObSQLSessionInfo* session = NULL;
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is NULL", K(ret));
      } else if (OB_FAIL(ob_datum_to_ob_time_with_date(*x_datum,
                     arg_type,
                     get_timezone_info(session),
                     ob_time,
                     get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx())))) {
        LOG_WARN("failed to convert obj to ob time", K(ret), K(*x_datum));
      } else {
        int64_t dt = 0;
        ObTimeConvertCtx cvrt_ctx(TZ_INFO(session), false);
        if (OB_FAIL(ObExprTRDateFormat::trunc_new_obtime(ob_time, fmt_str))) {
          LOG_WARN("fail to calc datetime", K(ret), K(fmt_str));
        } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, dt))) {
          LOG_WARN("fail to cast ob_time to datetime", K(ret), K(fmt_str), K(ob_time));
        } else {
          res_datum.set_datetime(dt);
        }
      }
    }
  }
  return ret;
}

int calc_trunc_expr_numeric(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // trunc(x, d)
  ObDatum* x_datum = NULL;
  ObDatum* d_datum = NULL;
  const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
  const ObObjType res_type = expr.datum_meta_.type_;
  if (OB_UNLIKELY(1 != expr.arg_cnt_ && 2 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_UNLIKELY(arg_type != res_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg_type of res_type", K(ret), K(arg_type), K(res_type));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum))) {
    LOG_WARN("eval arg 0 failed", K(ret));
  } else if (x_datum->is_null()) {
    res_datum.set_null();
  } else if (2 == expr.arg_cnt_ && OB_FAIL(expr.args_[1]->eval(ctx, d_datum))) {
    LOG_WARN("eval arg 1 failed", K(ret));
  } else if (2 == expr.arg_cnt_ && d_datum->is_null()) {
    res_datum.set_null();
  } else {
    int64_t scale = 0;
    if (2 == expr.arg_cnt_) {
      ObNumStackOnceAlloc tmp_alloc;
      number::ObNumber tmp_nmb(d_datum->get_number());
      number::ObNumber scale_nmb;
      if (OB_UNLIKELY(ObNumberType != arg_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arg type have to be number when arg cnt is 2", K(ret), K(arg_type));
      } else if (OB_FAIL(scale_nmb.from(tmp_nmb, tmp_alloc))) {
        LOG_WARN("get number failed", K(ret));
      } else if (OB_FAIL(scale_nmb.extract_valid_int64_with_trunc(scale))) {
        LOG_WARN("get int64 from scale_nmb failed", K(ret), K(scale_nmb));
      } else {
        scale = GET_SCALE_FOR_CALC(scale);
      }
    }
    if (OB_SUCC(ret)) {
      switch (res_type) {
        case ObNumberType: {
          number::ObNumber arg_nmb(x_datum->get_number());
          number::ObNumber res_nmb;
          ObNumStackOnceAlloc tmp_alloc;
          if (OB_FAIL(res_nmb.from(arg_nmb, tmp_alloc))) {
            LOG_WARN("get nmb from arg failed", K(ret), K(arg_nmb));
          } else if (OB_FAIL(res_nmb.trunc(scale))) {
            LOG_WARN("trunc number failed", K(ret), K(arg_nmb), K(scale));
          } else {
            res_datum.set_number(res_nmb);
          }
          break;
        }
        case ObFloatType: {
          // when res_type/arg_type is not number, arg cnt is 1. see calc_result_typeN().
          if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("arg cnt have to be 1", K(ret), K(res_type));
          } else {
            res_datum.set_float(truncf(x_datum->get_float()));
          }
          break;
        }
        case ObDoubleType: {
          // when res_type/arg_type is not number, arg cnt is 1. see calc_result_typeN().
          if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("arg cnt have to be 1", K(ret), K(res_type));
          } else {
            res_datum.set_double(trunc(x_datum->get_double()));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected res_type", K(ret), K(res_type));
          break;
        }
      }
    }
  }
  return ret;
}

int ObExprOracleTrunc::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (ObDateTimeType == rt_expr.args_[0]->datum_meta_.type_) {
    rt_expr.eval_func_ = calc_trunc_expr_datetime;
  } else {
    rt_expr.eval_func_ = calc_trunc_expr_numeric;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
