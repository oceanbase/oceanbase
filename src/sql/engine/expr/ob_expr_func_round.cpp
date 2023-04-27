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
#include "sql/engine/expr/ob_expr_func_round.h"
#include <string.h>
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
#define GET_SCALE_FOR_CALC(scale) ((!lib::is_oracle_mode()) \
    ? (scale < 0 ? max(ROUND_MIN_SCALE, scale) : min(ROUND_MAX_SCALE, scale)) \
    : (scale < 0 ? max(OB_MIN_NUMBER_SCALE, scale) : min(OB_MAX_NUMBER_SCALE, scale)))
#define GET_SCALE_FOR_DEDUCE(scale) ((!lib::is_oracle_mode()) \
    ? (scale < 0 ? 0 : min(ROUND_MAX_SCALE, scale)) \
    : (scale < 0 ? max(OB_MIN_NUMBER_SCALE, scale) : min(OB_MAX_NUMBER_SCALE, scale)))
namespace oceanbase
{
namespace sql
{

ObExprFuncRound::ObExprFuncRound(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_ROUND, N_ROUND, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprFuncRound::~ObExprFuncRound()
{
}

int ObExprFuncRound::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *params,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_UNLIKELY(NULL == params || param_num <= 0 || param_num > 2) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(params), K(param_num), K(type_ctx.get_session()));
  } else {
    OZ(se_deduce_type(type, params, param_num, type_ctx));
  }
  return ret;
}

int ObExprFuncRound::se_deduce_type(ObExprResType &type,
                                    ObExprResType *params,
                                    int64_t param_num,
                                    ObExprTypeCtx &type_ctx) const
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

int ObExprFuncRound::set_res_and_calc_type(ObExprResType *params, int64_t param_num,
                                       ObObjType &res_type)
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

int ObExprFuncRound::set_res_scale_prec(ObExprTypeCtx &type_ctx, ObExprResType *params,
                                        int64_t param_num, const ObObjType &res_type,
                                        ObExprResType &type)
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  ObObjTypeClass res_tc = ob_obj_type_class(res_type);
  const bool is_oracle = lib::is_oracle_mode();
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
      res_scale = DEFAULT_SCALE_FOR_INTEGER; // compatible with mysql
    } else if (is_oracle_date) {
      res_scale = DEFAULT_SCALE_FOR_DATE;
    } else if (lib::is_mysql_mode() && 2 == param_num && params[1].is_literal()
               && !params[0].is_integer_type()) {
      // oracle mode return number type, scale is ORA_NUMBER_SCALE_UNKNOWN_YET
      // here is only for mysql mode
      const ObObj &obj = params[1].get_param();
      ObArenaAllocator oballocator(ObModIds::BLOCK_ALLOC);
      ObCastMode cast_mode = CM_NONE;
      ObCollationType cast_coll_type = type_ctx.get_coll_type();
      const ObDataTypeCastParams dtc_params =
        ObBasicSessionInfo::create_dtc_params(type_ctx.get_session());
      if (OB_FAIL(ObSQLUtils::get_default_cast_mode(type_ctx.get_session(), cast_mode))) {
        LOG_WARN("failed to get default cast mode", K(ret));
      } else {
        cast_mode |= CM_WARN_ON_FAIL;
      }
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
    if (!is_oracle_mode()) {
      if (ob_is_number_tc(res_type)) {
        ObPrecision tmp_res_prec = -1;
        if (1 == param_num) {
          tmp_res_prec = static_cast<ObPrecision>(params[0].get_precision() -
                                                  params[0].get_scale() + 1);
          res_prec = tmp_res_prec >= 0 ? tmp_res_prec : res_prec;
          res_scale = 0;
        } else {
          tmp_res_prec = static_cast<ObPrecision>(params[0].get_precision() -
                                                  params[0].get_scale() + res_scale + 1);
        }
        res_prec = tmp_res_prec >= 0 ? tmp_res_prec : res_prec;
      } else if (ob_is_real_type(res_type)) {
        res_prec = (SCALE_UNKNOWN_YET == res_scale) ?
          PRECISION_UNKNOWN_YET : ObMySQLUtil::float_length(res_scale);
      }
    }
    type.set_scale(res_scale);
    type.set_precision(res_prec);
  }
  return ret;
}

static int do_round_by_type(const int64_t scale, const ObObjType &x_type,
                            const ObObjType &res_type, const ObDatum &x_datum,
                            ObEvalCtx &ctx, ObDatum &res_datum)
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

/**
 * @brief Check whether the batch has a null value, 
 * and check the skip flags and eval flags of the round expression.
 * If there is no null value and the skip flags and eval flags are both false, 
 * the function returns true, otherwise it returns false.
 * 
 * @param x_datums 
 * @param skip 
 * @param eval_flags 
 * @param batch_size 
 * @return true 
 * @return false 
 */
static bool is_batch_need_cal_all(const ObDatum *x_datums,
                            const ObBitVector &skip,
                            const ObBitVector &eval_flags,
                            const int64_t batch_size)
{
  bool ret = ObBitVector::bit_op_zero(skip, eval_flags, batch_size, 
                                      [](uint64_t l, uint64_t r) { return l | r; });
  for (int64_t i = 0; ret && i < batch_size; ++i) {
    ret = !(x_datums[i].is_null());
  }
  return ret;
}

static int do_round_by_type_batch_with_check(const int64_t scale, const ObExpr &expr,
                                  ObEvalCtx &ctx, const ObBitVector &skip,
                                  const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDatum *results = expr.locate_batch_datums(ctx);
  ObDatum *x_datums = expr.args_[0]->locate_batch_datums(ctx);
  const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
  switch (x_type) {
    case ObNumberType:
    case ObUNumberType: {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        if (skip.at(i) || eval_flags.at(i)) {
            continue;
        }
        ObDatum &x_datum = x_datums[i];
        eval_flags.set(i);
        if (x_datum.is_null()) {
          results[i].set_null();
        } else{
          const number::ObNumber x_nmb(x_datum.get_number());
          number::ObNumber res_nmb;
          ObNumStackOnceAlloc tmp_alloc;
          if (OB_FAIL(res_nmb.from(x_nmb, tmp_alloc))) {
            LOG_WARN("get num from x failed", K(ret), K(x_nmb));
            break;
          } else if (OB_FAIL(res_nmb.round(GET_SCALE_FOR_CALC(scale)))) {
            LOG_WARN("eval round of res_nmb failed", K(ret), K(scale), K(res_nmb));
            break;
          } else {
            results[i].set_number(res_nmb);
          }
        }
      }
      break;
    }
    case ObFloatType: {
      for (int64_t i = 0; i < batch_size; ++i) {
        if (skip.at(i) || eval_flags.at(i)) {
            continue;
        }
        ObDatum &x_datum = x_datums[i];
        eval_flags.set(i);
        if (x_datum.is_null()) {
          results[i].set_null();
        } else{
          // if in Oracle mode, param_num must be 1(scale is 0)
          // MySQL mode cannot be here. because if param type is float, calc type will be double.
          results[i].set_float(ObExprUtil::round_double(x_datum.get_float(), scale));
        }
      }
      break;
    }
    case ObDoubleType: {
      for (int64_t i = 0; i < batch_size; ++i) {
        if (skip.at(i) || eval_flags.at(i)) {
            continue;
        }
        ObDatum &x_datum = x_datums[i];
        eval_flags.set(i);
        if (x_datum.is_null()) {
          results[i].set_null();
        } else{
          // if in Oracle mode, param_num must be 1(scale is 0)
          results[i].set_double(ObExprUtil::round_double(x_datum.get_double(), scale));
        }
      }
      break;
    }
    case ObIntType: {
      for (int64_t i = 0; i < batch_size; ++i) {
        if (skip.at(i) || eval_flags.at(i)) {
            continue;
        }
        ObDatum &x_datum = x_datums[i];
        eval_flags.set(i);
        if (x_datum.is_null()) {
          results[i].set_null();
        } else{
          int64_t x_int = x_datum.get_int();
          bool neg = x_int < 0;
          x_int = neg ? -x_int : x_int;
          int64_t res_int = static_cast<int64_t>(ObExprUtil::round_uint64(x_int, scale));
          res_int = neg ? -res_int : res_int;
          results[i].set_int(res_int);
        }
      }
      break;
    }
    case ObUInt64Type: {
      for (int64_t i = 0; i < batch_size; ++i) {
        if (skip.at(i) || eval_flags.at(i)) {
            continue;
        }
        ObDatum &x_datum = x_datums[i];
        eval_flags.set(i);
        if (x_datum.is_null()) {
          results[i].set_null();
        } else{
          uint64_t x_uint = x_datum.get_uint();
          uint64_t res_uint = ObExprUtil::round_uint64(x_uint, scale);
          results[i].set_uint(res_uint);
        }
      }
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

static int do_round_by_type_batch_without_check(const int64_t scale, const ObExpr &expr,
                                  ObEvalCtx &ctx, const int64_t batch_size)
{
  // This function only calculates batch that do not contain null value
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDatum *results = expr.locate_batch_datums(ctx);
  ObDatum *x_datums = expr.args_[0]->locate_batch_datums(ctx);
  const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
  switch (x_type) {
    case ObNumberType:
    case ObUNumberType: {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        const number::ObNumber x_nmb(x_datums[i].get_number());
        number::ObNumber res_nmb;
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_FAIL(res_nmb.from(x_nmb, tmp_alloc))) {
          LOG_WARN("get num from x failed", K(ret), K(x_nmb));
        } else if (OB_FAIL(res_nmb.round(GET_SCALE_FOR_CALC(scale)))) {
          LOG_WARN("eval round of res_nmb failed", K(ret), K(scale), K(res_nmb));
        } else {
          results[i].set_number(res_nmb);
        }
      }
      break;
    }
    case ObFloatType: {
      for (int64_t i = 0; i < batch_size; ++i) {
        // if in Oracle mode, param_num must be 1(scale is 0)
        // MySQL mode cannot be here. because if param type is float, calc type will be double.
        results[i].set_float(ObExprUtil::round_double(x_datums[i].get_float(), scale));
      }
      break;
    }
    case ObDoubleType: {
      for (int64_t i = 0; i < batch_size; ++i) {
        // if in Oracle mode, param_num must be 1(scale is 0)
        results[i].set_double(ObExprUtil::round_double(x_datums[i].get_double(), scale));
      }
      break;
    }
    case ObIntType: {
      for (int64_t i = 0; i < batch_size; ++i) {
        int64_t x_int = x_datums[i].get_int();
        bool neg = x_int < 0;
        x_int = neg ? -x_int : x_int;
        int64_t res_int = static_cast<int64_t>(ObExprUtil::round_uint64(x_int, scale));
        res_int = neg ? -res_int : res_int;
        results[i].set_int(res_int);
      }
      break;
    }
    case ObUInt64Type: {
      for (int64_t i = 0; i < batch_size; ++i) {
        uint64_t x_uint = x_datums[i].get_uint();
        uint64_t res_uint = ObExprUtil::round_uint64(x_uint, scale);
        results[i].set_uint(res_uint);
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg type", K(ret), K(x_type));
      break;
    }
  }
  if (OB_SUCC(ret)) {
    eval_flags.set_all(batch_size);
  }
  return ret;
}

int calc_round_expr_numeric1(const sql::ObExpr &expr, sql::ObEvalCtx &ctx,
                              sql::ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *x_datum = NULL;
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

int ObExprFuncRound::calc_round_expr_numeric1_batch(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else {
    ObDatum *x_datums = expr.args_[0]->locate_batch_datums(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    if (is_batch_need_cal_all(x_datums, skip, eval_flags, batch_size)) {
      if (OB_FAIL(do_round_by_type_batch_without_check(0, expr, ctx, batch_size))) {
        const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
        LOG_WARN("calc round by type failed", K(ret), K(x_type), K(expr));
      }
    } else {
      if (OB_FAIL(do_round_by_type_batch_with_check(0, expr, ctx, skip, batch_size))) {
        const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
        LOG_WARN("calc round by type failed", K(ret), K(x_type), K(expr));
      }
    }
  }
  return ret;
}

int calc_round_expr_numeric2(const sql::ObExpr &expr, sql::ObEvalCtx &ctx,
                              sql::ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *x_datum = NULL;
  ObDatum *fmt_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
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

int ObExprFuncRound::calc_round_expr_numeric2_batch(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatum *fmt_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else {
    int64_t scale = 0;
    // get scale
    const ObObjType fmt_type = expr.args_[1]->datum_meta_.type_;
    if (fmt_datum->is_null()) {
      // do nothing
    } else if (ObNumberType == fmt_type) {
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
      if (fmt_datum->is_null()) {
        ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
        ObDatum *results = expr.locate_batch_datums(ctx);
        for (int64_t i = 0; i < batch_size; ++i) {
          eval_flags.set(i);
          results[i].set_null();
        }
      } else {
        ObDatum *x_datums = expr.args_[0]->locate_batch_datums(ctx);
        ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
        if (is_batch_need_cal_all(x_datums, skip, eval_flags, batch_size)) {
          if (OB_FAIL(do_round_by_type_batch_without_check(scale, expr, ctx, batch_size))) {
            const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
            LOG_WARN("calc round by type failed", K(ret), K(x_type), K(expr));
          }
        } else {
          if (OB_FAIL(do_round_by_type_batch_with_check(scale, expr, ctx, skip, batch_size))) {
            const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
            LOG_WARN("calc round by type failed", K(ret), K(x_type), K(expr));
          }
        }
      }
    }
  }
  return ret;
}

int calc_round_expr_datetime_inner(const ObDatum &x_datum, const ObString &fmt_str,
                                   ObEvalCtx &ctx, int64_t &dt,
                                   const sql::ObExpr &expr)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  const ObTimeZoneInfo *tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());
  if (OB_FAIL(ob_datum_to_ob_time_with_date(x_datum, ObDateTimeType,
                              tz_info, ob_time,
                              get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), false, 0, false))) {
    LOG_WARN("ob_datum_to_ob_time_with_date failed", K(ret));
  } else {
    ObTimeConvertCtx cvrt_ctx(TZ_INFO(ctx.exec_ctx_.get_my_session()), false);
    if (expr.arg_cnt_ > 1 && !!(expr.args_[1]->is_static_const_)) {
      auto rt_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
      ObExprSingleFormatCtx *single_fmt_ctx = NULL;
      if (NULL == (single_fmt_ctx = static_cast<ObExprSingleFormatCtx *>
                   (ctx.exec_ctx_.get_expr_op_ctx(rt_ctx_id)))) {
        if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(rt_ctx_id, single_fmt_ctx))) {
          LOG_WARN("failed to create operator ctx", K(ret));
        } else if (OB_FAIL(ObExprTRDateFormat::get_format_id_by_format_string(
                             fmt_str, single_fmt_ctx->fmt_id_))) {
          LOG_WARN("fail to get format id by format string", K(ret));
        }
      }
      OZ (ObExprTRDateFormat::round_new_obtime_by_fmt_id(ob_time, single_fmt_ctx->fmt_id_));
    } else {
      OZ (ObExprTRDateFormat::round_new_obtime(ob_time, fmt_str));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, dt))) {
        LOG_WARN("fail to cast ob_time to datetime", K(ret), K(fmt_str));
      }
    }
  }
  return ret;
}

int calc_round_expr_datetime1(const sql::ObExpr &expr, sql::ObEvalCtx &ctx,
                              sql::ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  int64_t dt = 0;
  ObDatum *x_datum = NULL;
  ObString fmt_str("DD");
  if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (x_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(calc_round_expr_datetime_inner(*x_datum, fmt_str, ctx, dt, expr))) {
    LOG_WARN("calc_round_expr_datetime_inner failed", K(ret));
  } else {
    res_datum.set_datetime(dt);
  }
  return ret;
}

int ObExprFuncRound::calc_round_expr_datetime1_batch(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObString fmt_str("DD");
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("eval arg failed", K(ret), K(expr));
  } else {
    ObDatum *results = expr.locate_batch_datums(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      int64_t dt = 0;
      ObDatum &x_datum = expr.args_[0]->locate_expr_datum(ctx, i);
      eval_flags.set(i);
      if (x_datum.is_null()) {
        results[i].set_null();
      } else if (OB_FAIL(calc_round_expr_datetime_inner(x_datum, fmt_str, ctx, dt, expr))) {
        LOG_WARN("calc_round_expr_datetime_inner failed", K(ret));
      } else {
        results[i].set_datetime(dt);
      }
    }
  }
  return ret;
}

int calc_round_expr_datetime2(const sql::ObExpr &expr, sql::ObEvalCtx &ctx,
                              sql::ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  int64_t dt = 0;
  ObDatum *x_datum = NULL;
  ObDatum *fmt_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (x_datum->is_null() || fmt_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(calc_round_expr_datetime_inner(*x_datum, fmt_datum->get_string(),
                                                    ctx, dt, expr))) {
    LOG_WARN("calc_round_expr_datetime_inner failed", K(ret));
  } else {
    res_datum.set_datetime(dt);
  }
  return ret;
}

int ObExprFuncRound::calc_round_expr_datetime2_batch(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDatum *fmt_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else {
    ObDatum *results = expr.locate_batch_datums(ctx);
    ObDatum *x_datums = expr.args_[0]->locate_batch_datums(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      int64_t dt = 0;
      ObDatum &x_datum = x_datums[i];
      eval_flags.set(i);
      if (x_datum.is_null() || fmt_datum->is_null()) {
        results[i].set_null();
      } else if (OB_FAIL(calc_round_expr_datetime_inner(x_datum, fmt_datum->get_string(), ctx, dt,
                                                        expr))) {
        LOG_WARN("calc_round_expr_datetime_inner failed", K(ret));
      } else {
        results[i].set_datetime(dt);
      }
    }
  }
  return ret;
}

int ObExprFuncRound::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  // round(x, fmt)
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    const ObObjType &x_type = rt_expr.args_[0]->datum_meta_.type_;
    const ObObjType &res_type = rt_expr.datum_meta_.type_;
    if (OB_UNLIKELY(x_type != res_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid arg type or res type", K(ret), K(x_type), K(res_type));
    } else if (2 == rt_expr.arg_cnt_) {
      const ObObjType fmt_type = rt_expr.args_[1]->datum_meta_.type_;
      if (is_oracle_mode()) {
        if (ObDateTimeType == x_type && ObVarcharType == fmt_type) {
          rt_expr.eval_func_ = calc_round_expr_datetime2;
          // Only implement vectorization when parameter 0 is batch and parameter 1 is constant
          if (rt_expr.args_[0]->is_batch_result() && !(rt_expr.args_[1]->is_batch_result())) {
            rt_expr.eval_batch_func_ = calc_round_expr_datetime2_batch;
          }
        } else {
          rt_expr.eval_func_ = calc_round_expr_numeric2;
          // Only implement vectorization when parameter 0 is batch and parameter 1 is constant
          if (rt_expr.args_[0]->is_batch_result() && !(rt_expr.args_[1]->is_batch_result())) {
            rt_expr.eval_batch_func_ = calc_round_expr_numeric2_batch;
          }
        }
      } else {
        rt_expr.eval_func_ = calc_round_expr_numeric2;
        // Only implement vectorization when parameter 0 is batch and parameter 1 is constant
        if (rt_expr.args_[0]->is_batch_result() && !(rt_expr.args_[1]->is_batch_result())) {
          rt_expr.eval_batch_func_ = calc_round_expr_numeric2_batch;
        }
      }
    } else {
      if (ObDateTimeType == x_type) {
        rt_expr.eval_func_ = calc_round_expr_datetime1;
        rt_expr.eval_batch_func_ = calc_round_expr_datetime1_batch;
      } else {
        rt_expr.eval_func_ = calc_round_expr_numeric1;
        rt_expr.eval_batch_func_ = calc_round_expr_numeric1_batch;
      }
    }
  }
  return ret;
}

} // sql
} // oceanbase
