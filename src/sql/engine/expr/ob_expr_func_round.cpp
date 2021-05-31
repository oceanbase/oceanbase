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
#include "sql/engine/expr/ob_expr_func_round.h"
#include <string.h>
#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
#define GET_SCALE_FOR_CALC(scale)                                                                     \
  ((!lib::is_oracle_mode()) ? (scale < 0 ? max(ROUND_MIN_SCALE, scale) : min(ROUND_MAX_SCALE, scale)) \
                            : (scale < 0 ? max(OB_MIN_NUMBER_SCALE, scale) : min(OB_MAX_NUMBER_SCALE, scale)))
#define GET_SCALE_FOR_DEDUCE(scale)                                         \
  ((!lib::is_oracle_mode()) ? (scale < 0 ? 0 : min(ROUND_MAX_SCALE, scale)) \
                            : (scale < 0 ? max(OB_MIN_NUMBER_SCALE, scale) : min(OB_MAX_NUMBER_SCALE, scale)))
namespace oceanbase {
namespace sql {

ObExprFuncRound::ObExprFuncRound(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ROUND, N_ROUND, ONE_OR_TWO, NOT_ROW_DIMENSION)
{}

ObExprFuncRound::~ObExprFuncRound()
{}

int ObExprFuncRound::calc_result_typeN(
    ObExprResType& type, ObExprResType* params, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (OB_UNLIKELY(NULL == params || param_num <= 0 || param_num > 2) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(params), K(param_num), K(type_ctx.get_session()));
  } else if (session->use_static_typing_engine()) {
    OZ(se_deduce_type(type, params, param_num, type_ctx));
  } else {  // old engine type deduce
    if (1 == param_num) {
      if (share::is_oracle_mode()) {
        ObObjType result_type = ObMaxType;
        if (OB_FAIL(ObExprResultTypeUtil::get_round_result_type(result_type, params[0].get_type()))) {
          LOG_WARN("fail to get_round_result_type", K(ret), K(params[0].get_type()));
        } else {
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
          }
        }
      } else {
        type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
        // set calc type
        params[0].set_calc_type(ObNumberType);
      }
    } else {
      // TODO:: these code were too bad, need reconsitution
      const bool is_oracle = share::is_oracle_mode();
      const bool is_oracle_date =
          is_oracle && (ObDateTimeTC == params[0].get_type_class() || ObOTimestampTC == params[0].get_type_class());
      if (params[1].is_null()) {
        type.set_scale(DEFAULT_SCALE_FOR_INTEGER);  // compatible with mysql.
      } else if (is_oracle_date) {
        type.set_scale(DEFAULT_SCALE_FOR_DATE);
        type.set_type(ObDateTimeType);
        params[0].set_calc_type(ObDateTimeType);
        params[1].set_calc_type(ObVarcharType);
      } else if (params[1].is_literal()) {  // literal
        const ObObj& obj = params[1].get_param();
        ObArenaAllocator oballocator(ObModIds::BLOCK_ALLOC);
        ObCastMode cast_mode = CM_WARN_ON_FAIL;
        ObCollationType cast_coll_type = type_ctx.get_coll_type();
        const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(type_ctx.get_session());
        ObCastCtx cast_ctx(&oballocator, &dtc_params, 0, cast_mode, cast_coll_type);
        int64_t scale = 0;
        EXPR_GET_INT64_V2(obj, scale);
        if (OB_SUCC(ret)) {
          type.set_scale(static_cast<int16_t>(GET_SCALE_FOR_DEDUCE(obj.get_int())));
        }
      } else {
        type.set_scale(SCALE_UNKNOWN_YET);
      }
      if (!is_oracle_date) {
        // set calc_type
        if (is_oracle) {
          params[0].set_calc_type(ObNumberType);
          params[1].set_calc_type(ObNumberType);
          type.set_type(ObNumberType);
          type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
          type.set_precision(PRECISION_UNKNOWN_YET);
          type.set_calc_scale(0);
        } else {
          params[0].set_calc_type(ObNumberType);
          params[1].set_calc_type(ObIntType);
        }
      }
    }

    if (OB_SUCC(ret) && !lib::is_oracle_mode()) {
      // integer->integer,double->double,number->number else -> double
      if (params[0].is_numeric_type()) {
        if (params[0].get_type() <= ObIntType) {
          type.set_int();
          type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
          type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
        } else if (params[0].get_type() <= ObUInt64Type || ObBitType == params[0].get_type()) {
          type.set_uint64();
          type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
          type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
        } else if (params[0].get_type() <= ObDoubleType || ObEnumType == params[0].get_type() ||
                   ObSetType == params[0].get_type()) {
          type.set_double();
        } else {
          type.set_number();
          if (1 == param_num) {
            // 1 for carry
            type.set_precision(static_cast<ObPrecision>((params[0].get_precision() - params[0].get_scale() + 1)));
            type.set_scale(0);
          } else {
            // 1 for carry
            ObPrecision precision =
                static_cast<ObPrecision>(params[0].get_precision() - params[0].get_scale() + type.get_scale() + 1);
            type.set_precision(precision);
          }
        }
        if (params[0].get_type() >= ObTinyIntType && params[0].get_type() <= ObUInt64Type) {
          type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
        }
      } else if (share::is_oracle_mode() &&
                 (ObDateTimeTC == params[0].get_type_class() || ObOTimestampTC == params[0].get_type_class())) {
        // already set
      } else {
        type.set_double();
      }
    }
    ObExprOperator::calc_result_flag1(type, params[0]);
  }
  return ret;
}

int ObExprFuncRound::set_round_val(number::ObNumber& nmb, ObObj& result, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_ctx);
  switch (result_type_.get_type()) {
    case ObIntType: {
      int64_t val = 0;
      if (nmb.is_valid_int64(val)) {
        result.set_int(val);
      } else {
        result.set_number(nmb);
      }
      break;
    }
    case ObUInt64Type: {
      uint64_t uval = 0;
      if (nmb.is_valid_uint64(uval)) {
        result.set_uint64(uval);
      } else {
        result.set_number(nmb);
      }
      break;
    }
    case ObNumberType: {
      result.set_number(nmb);
      break;
    }
    default: {
      break;
    }
  }
  return ret;
}

int ObExprFuncRound::calc_result1(ObObj& result, const ObObj& input, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (input.is_null()) {
    result.set_null();
  } else if (input.is_number()) {
    ObObj default_format;
    default_format.set_int(0);
    ret = calc_with_decimal(result, input, default_format, expr_ctx);
  } else if (share::is_oracle_mode()) {
    if (input.is_datetime()) {
      ObObj default_format;
      default_format.set_varchar("DD");
      ret = calc_with_date(result, input, default_format, expr_ctx);
    } else if (input.is_float()) {
      result.set_float(roundf(input.get_float()));
    } else if (input.is_double()) {
      result.set_double(round(input.get_double()));
    } else {
      ret = OB_INVALID_NUMERIC;
      LOG_WARN("invalid numeric", K(ret), K(input));
    }
  } else {
    ret = OB_INVALID_NUMERIC;
    LOG_WARN("invalid numeric. number is expected", K(ret), K(input));
  }
  return ret;
}

int ObExprFuncRound::calc_result2(ObObj& result, const ObObj& input, const ObObj& param, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init.", K(ret));
  } else if (input.is_null() || param.is_null()) {
    result.set_null();
  } else if (share::is_oracle_mode()) {
    if (ObDateTimeTC == input.get_type_class()) {
      ret = calc_with_date(result, input, param, expr_ctx);
    } else if (ObNumberTC == input.get_type_class()) {
      ret = calc_with_decimal(result, input, param, expr_ctx);
    } else {
      ret = OB_INVALID_NUMERIC;
      LOG_WARN("Invalid numeric. trunc expected number or date", K(ret), K(input), K(param));
    }
  } else {
    if (OB_LIKELY(ObNumberTC == input.get_type_class())) {
      ret = calc_with_decimal(result, input, param, expr_ctx);
    } else {
      ret = OB_INVALID_NUMERIC;
      LOG_WARN("Invalid numeric. trunc expected number or date", K(ret), K(input), K(param));
    }
  }
  return ret;
}

int ObExprFuncRound::calc_resultN(ObObj& result, const ObObj* objs, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (1 == param_num) {
    ret = calc_result1(result, objs[0], expr_ctx);
  } else if (2 == param_num) {
    ret = calc_result2(result, objs[0], objs[1], expr_ctx);
  } else {
    ret = OB_INVALID_NUMERIC;
    LOG_WARN("invalid argument number for round().", K(param_num));
  }
  return ret;
}

int ObExprFuncRound::se_deduce_type(
    ObExprResType& type, ObExprResType* params, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjType res_type = ObMaxType;
  if (OB_FAIL(set_res_and_calc_type(params, param_num, res_type))) {
    LOG_WARN("set_calc_type for round expr failed", K(ret), K(res_type), K(param_num));
  } else if (OB_FAIL(set_res_scale_prec(type_ctx, params, param_num, res_type, type))) {
    LOG_WARN("set_res_scale_prec round expr failed", K(ret), K(res_type), K(param_num));
  } else {
    ObExprOperator::calc_result_flag1(type, params[0]);
    type.set_type(res_type);
  }
  return ret;
}

int ObExprFuncRound::set_res_and_calc_type(ObExprResType* params, int64_t param_num, ObObjType& res_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprResultTypeUtil::get_round_result_type(res_type, params[0].get_type()))) {
    // res_type can be(mysql mode): ObNumberType, ObUNumberType, ObDoubleType,
    //                              ObIntType, ObUInt64Type
    // res_type can be(oracle mode): ObNumberType, ObDoubleType, ObFloatType
    //                               ObDateTimeType
    LOG_WARN("fail to get_round_result_type", K(ret), K(params[0].get_type()));
  } else if (1 == param_num) {
    params[0].set_calc_type(res_type);
  } else if (2 == param_num) {
    if (lib::is_oracle_mode()) {
      if (ObDateTimeType == res_type || ObOTimestampTC == ob_obj_type_class(res_type)) {
        params[0].set_calc_type(ObDateTimeType);
        params[1].set_calc_type(ObVarcharType);
      } else {
        // always be ObNumberType
        res_type = ObNumberType;
        params[0].set_calc_type(res_type);
        params[1].set_calc_type(res_type);
      }
    } else {
      params[0].set_calc_type(res_type);
      params[1].set_calc_type(ObIntType);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param_num", K(ret), K(param_num));
  }
  return ret;
}

int ObExprFuncRound::set_res_scale_prec(
    ObExprTypeCtx& type_ctx, ObExprResType* params, int64_t param_num, const ObObjType& res_type, ObExprResType& type)
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  ObObjTypeClass res_tc = ob_obj_type_class(res_type);
  const bool is_oracle = share::is_oracle_mode();
  const bool is_oracle_date = is_oracle && (ObDateTimeTC == res_tc || ObOTimestampTC == res_tc);
  ObPrecision res_prec = PRECISION_UNKNOWN_YET;
  ObScale res_scale = is_oracle ? ORA_NUMBER_SCALE_UNKNOWN_YET : SCALE_UNKNOWN_YET;

  if (OB_UNLIKELY(1 != param_num && 2 != param_num)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param_num", K(ret), K(param_num));
  } else {
    if (1 == param_num && lib::is_mysql_mode()) {
      res_scale = DEFAULT_SCALE_FOR_INTEGER;
    } else if (2 == param_num && params[1].is_null()) {
      res_scale = DEFAULT_SCALE_FOR_INTEGER;  // compatible with mysql
    } else if (is_oracle_date) {
      res_scale = DEFAULT_SCALE_FOR_DATE;
    } else if (lib::is_mysql_mode() && 2 == param_num && params[1].is_literal() && !params[0].is_integer_type()) {
      // oracle mode return number type, scale is ORA_NUMBER_SCALE_UNKNOWN_YET
      // here is only for mysql mode
      const ObObj& obj = params[1].get_param();
      ObArenaAllocator oballocator(ObModIds::BLOCK_ALLOC);
      ObCastMode cast_mode = CM_WARN_ON_FAIL;
      ObCollationType cast_coll_type = type_ctx.get_coll_type();
      const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(type_ctx.get_session());
      ObCastCtx cast_ctx(&oballocator, &dtc_params, 0, cast_mode, cast_coll_type);
      int64_t scale = 0;
      EXPR_GET_INT64_V2(obj, scale);
      if (OB_SUCC(ret)) {
        res_scale = static_cast<ObScale>(GET_SCALE_FOR_DEDUCE(scale));
      } else {
        res_scale = static_cast<ObScale>(scale);
      }
    } else {
      if (lib::is_mysql_mode()) {
        if (ob_is_numeric_type(res_type)) {
          if (ob_is_int_tc(res_type)) {
            res_prec = ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_;
            res_scale = ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_;
          } else if (ob_is_uint_tc(res_type)) {
            res_prec = ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_;
            res_scale = ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_;
          } else {
            res_prec = params[0].get_precision();
            res_scale = params[0].get_scale();
          }
        }
      } else {
        res_scale = ORA_NUMBER_SCALE_UNKNOWN_YET;
        res_prec = PRECISION_UNKNOWN_YET;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!is_oracle_mode() && ob_is_number_tc(res_type)) {
      ObPrecision tmp_res_prec = -1;
      if (1 == param_num) {
        tmp_res_prec = static_cast<ObPrecision>(params[0].get_precision() - params[0].get_scale() + 1);
        res_prec = tmp_res_prec >= 0 ? tmp_res_prec : res_prec;
        res_scale = 0;
      } else {
        tmp_res_prec = static_cast<ObPrecision>(params[0].get_precision() - params[0].get_scale() + res_scale + 1);
      }
      res_prec = tmp_res_prec >= 0 ? tmp_res_prec : res_prec;
    }
    type.set_scale(res_scale);
    type.set_precision(res_prec);
  }
  return ret;
}

int ObExprFuncRound::calc_with_date(ObObj& result, const ObObj& source, const ObObj& format, ObExprCtx& expr_ctx) const
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
    if (OB_FAIL(ObExprTRDateFormat::round_new_obtime(ob_time, format.get_string()))) {
      LOG_WARN("fail to calc datetime", K(ret), K(source), K(format));
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, dt))) {
      LOG_WARN("fail to cast ob_time to datetime", K(ret), K(source), K(format));
    } else {
      result.set_datetime(dt);
    }
  }
  return ret;
}

int ObExprFuncRound::calc_with_decimal(ObObj& result, const ObObj& input, const ObObj& param, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t scale = 0;
  TYPE_CHECK(input, ObNumberType);
  if (lib::is_oracle_mode() && (ObNumberType == param.get_type())) {
    const number::ObNumber tmp = param.get_number();
    if (OB_FAIL(tmp.extract_valid_int64_with_trunc(scale))) {
      LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(tmp));
    }
  } else {
    TYPE_CHECK(param, ObIntType);
    scale = param.get_int();
  }

  if (OB_SUCC(ret)) {
    number::ObNumber num = input.get_number();
    number::ObNumber nmb;
    if (OB_FAIL(nmb.from(num, *expr_ctx.calc_buf_))) {
      LOG_WARN("copy number failed.", K(ret), K(num));
    } else if (result_type_.get_type() == ObDoubleType) {
      /**
       * for approximate-value numbers, ROUND() uses the "round to nearest even" rule:
       * A value with a fractional part exactly half way between two integers is rounded to
       * the nearest even integer. so here we do special treatment for ObDoubleType type.
       */
      double d_val = 0.0;
      ObObj tmp_obj;
      tmp_obj.set_number(nmb);
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_DOUBLE_V2(tmp_obj, d_val);
      if (OB_SUCC(ret)) {
        result.set_double(ObExprUtil::round_double(d_val, scale));
      }
    } else {
      if (OB_FAIL(nmb.round(GET_SCALE_FOR_CALC(scale)))) {
        LOG_WARN("round failed.", K(ret), K(nmb.format()), K(scale));
      } else {
        ret = set_round_val(nmb, result, expr_ctx);
      }
    }
  }
  return ret;
}

static int do_round_by_type(const int64_t scale, const ObObjType& x_type, const ObObjType& res_type,
    const ObDatum& x_datum, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(res_type);
  UNUSED(ctx);
  switch (x_type) {
    case ObNumberType:
    case ObUNumberType: {
      const number::ObNumber x_nmb(x_datum.get_number());
      number::ObNumber res_nmb;
      ObNumStackOnceAlloc tmp_alloc;
      if (OB_FAIL(res_nmb.from(x_nmb, tmp_alloc))) {
        LOG_WARN("get num from x failed", K(ret), K(x_nmb));
      } else if (OB_FAIL(res_nmb.round(GET_SCALE_FOR_CALC(scale)))) {
        LOG_WARN("eval round of res_nmb failed", K(ret), K(scale), K(res_nmb));
      } else {
        res_datum.set_number(res_nmb);
      }
      break;
    }
    case ObFloatType: {
      // if in Oracle mode, param_num must be 1(scale is 0)
      // MySQL mode cannot be here. because if param type is float, calc type will be double.
      res_datum.set_float(ObExprUtil::round_double(x_datum.get_float(), scale));
      break;
    }
    case ObDoubleType: {
      // if in Oracle mode, param_num must be 1(scale is 0)
      res_datum.set_double(ObExprUtil::round_double(x_datum.get_double(), scale));
      break;
    }
    case ObIntType: {
      int64_t x_int = x_datum.get_int();
      bool neg = x_int < 0;
      x_int = neg ? -x_int : x_int;
      int64_t res_int = static_cast<int64_t>(ObExprUtil::round_uint64(x_int, scale));
      res_int = neg ? -res_int : res_int;
      res_datum.set_int(res_int);
      break;
    }
    case ObUInt64Type: {
      uint64_t x_uint = x_datum.get_uint();
      uint64_t res_uint = ObExprUtil::round_uint64(x_uint, scale);
      res_datum.set_uint(res_uint);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg type", K(ret), K(x_type));
      break;
    }
  }
  return ret;
}

int calc_round_expr_numeric1(const sql::ObExpr& expr, sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* x_datum = NULL;
  const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
  const ObObjType res_type = expr.datum_meta_.type_;
  if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (x_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(do_round_by_type(0, x_type, res_type, *x_datum, ctx, res_datum))) {
    LOG_WARN("calc round by type failed", K(ret), K(x_type), K(expr));
  }
  return ret;
}

int calc_round_expr_numeric2(const sql::ObExpr& expr, sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* x_datum = NULL;
  ObDatum* fmt_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum)) || OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (x_datum->is_null() || fmt_datum->is_null()) {
    res_datum.set_null();
  } else {
    int64_t scale = 0;
    // get scale
    const ObObjType fmt_type = expr.args_[1]->datum_meta_.type_;
    if (ObNumberType == fmt_type) {
      const number::ObNumber fmt_nmb(fmt_datum->get_number());
      if (OB_FAIL(fmt_nmb.extract_valid_int64_with_trunc(scale))) {
        LOG_WARN("extract_valid_int64_with_trunc failed", K(ret), K(fmt_nmb));
      }
    } else if (ObIntType == fmt_type) {
      scale = fmt_datum->get_int();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected fmt type", K(ret), K(fmt_type), K(expr));
    }

    if (OB_SUCC(ret)) {
      const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
      const ObObjType res_type = expr.datum_meta_.type_;
      if (OB_FAIL(do_round_by_type(scale, x_type, res_type, *x_datum, ctx, res_datum))) {
        LOG_WARN("calc round by type failed", K(ret), K(x_type), K(expr));
      }
    }
  }
  return ret;
}

int calc_round_expr_datetime_inner(const ObDatum& x_datum, const ObString& fmt_str, ObEvalCtx& ctx, int64_t& dt)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  const ObTimeZoneInfo* tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());
  if (OB_FAIL(ob_datum_to_ob_time_with_date(
          x_datum, ObDateTimeType, tz_info, ob_time, get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx())))) {
    LOG_WARN("ob_datum_to_ob_time_with_date failed", K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(TZ_INFO(ctx.exec_ctx_.get_my_session()), false);
    if (OB_FAIL(ObExprTRDateFormat::round_new_obtime(ob_time, fmt_str))) {
      LOG_WARN("fail to calc datetime", K(ret), K(fmt_str));
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, dt))) {
      LOG_WARN("fail to cast ob_time to datetime", K(ret), K(fmt_str));
    }
  }
  return ret;
}

int calc_round_expr_datetime1(const sql::ObExpr& expr, sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int64_t dt = 0;
  ObDatum* x_datum = NULL;
  ObString fmt_str("DD");
  if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (x_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(calc_round_expr_datetime_inner(*x_datum, fmt_str, ctx, dt))) {
    LOG_WARN("calc_round_expr_datetime_inner failed", K(ret));
  } else {
    res_datum.set_datetime(dt);
  }
  return ret;
}

int calc_round_expr_datetime2(const sql::ObExpr& expr, sql::ObEvalCtx& ctx, sql::ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  int64_t dt = 0;
  ObDatum* x_datum = NULL;
  ObDatum* fmt_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum)) || OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (x_datum->is_null() || fmt_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(calc_round_expr_datetime_inner(*x_datum, fmt_datum->get_string(), ctx, dt))) {
    LOG_WARN("calc_round_expr_datetime_inner failed", K(ret));
  } else {
    res_datum.set_datetime(dt);
  }
  return ret;
}

int ObExprFuncRound::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  // round(x, fmt)
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    const ObObjType& x_type = rt_expr.args_[0]->datum_meta_.type_;
    const ObObjType& res_type = rt_expr.datum_meta_.type_;
    if (OB_UNLIKELY(x_type != res_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid arg type or res type", K(ret), K(x_type), K(res_type));
    } else if (2 == rt_expr.arg_cnt_) {
      const ObObjType fmt_type = rt_expr.args_[1]->datum_meta_.type_;
      if (is_oracle_mode()) {
        if (ObDateTimeType == x_type && ObVarcharType == fmt_type) {
          rt_expr.eval_func_ = calc_round_expr_datetime2;
        } else {
          rt_expr.eval_func_ = calc_round_expr_numeric2;
        }
      } else {
        rt_expr.eval_func_ = calc_round_expr_numeric2;
      }
    } else {
      if (ObDateTimeType == x_type) {
        rt_expr.eval_func_ = calc_round_expr_datetime1;
      } else {
        rt_expr.eval_func_ = calc_round_expr_numeric1;
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
