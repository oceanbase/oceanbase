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
      const ObDataTypeCastParams dtc_params = type_ctx.get_dtc_params();
      ObSQLUtils::get_default_cast_mode(type_ctx.get_sql_mode(), cast_mode);
      cast_mode |= CM_WARN_ON_FAIL;
      ObCastCtx cast_ctx(&oballocator, &dtc_params, 0, cast_mode, cast_coll_type);
      int64_t scale = 0;
      EXPR_GET_INT64_V2(obj, scale);
      if (OB_SUCC(ret)) {
        res_scale = static_cast<ObScale>(GET_SCALE_FOR_DEDUCE(scale));
      } else {
        res_scale = static_cast<ObScale>(scale);
      }
      if ((ob_is_number_tc(params[0].get_type()) || ob_is_decimal_int_tc(params[0].get_type()))
          && params[0].get_scale() < res_scale) {
        // eg : select round(123.123, 100); -> result is 123.123
        res_scale = params[0].get_scale();
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
      if (ob_is_number_tc(res_type) || ob_is_decimal_int_tc(res_type)) {
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
      } else if (ob_is_integer_type(res_type)) {
        if (PRECISION_UNKNOWN_YET == res_prec) {
          res_prec = ObAccuracy::DDL_DEFAULT_ACCURACY[res_type].precision_;
        }
      }
    }
    type.set_scale(res_scale);
    type.set_precision(res_prec);
  }
  return ret;
}

int ObExprFuncRound::do_round_decimalint(
    const int16_t in_prec, const int16_t in_scale,
    const int16_t out_prec, const int16_t out_scale, const int64_t round_scale,
    const ObDatum &in_datum, ObDecimalIntBuilder &res_val)
{
  int ret = OB_SUCCESS;
  const ObDecimalInt *decint = in_datum.get_decimal_int();
  const int32_t int_bytes = in_datum.get_int_bytes();
  if (in_scale != round_scale || get_decimalint_type(in_prec) != get_decimalint_type(out_prec)) {
    ObDecimalIntBuilder scaled_down_val;
    ObDecimalIntBuilder scaled_up_val;
    int32_t expected_int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
    if (OB_FAIL(wide::common_scale_decimalint(
                decint, int_bytes, in_scale, round_scale, scaled_down_val))) {
      LOG_WARN("scale decimal int failed", K(ret), K(int_bytes), K(in_scale), K(round_scale));
    } else if ((round_scale < out_scale)
               && OB_FAIL(wide::common_scale_decimalint(scaled_down_val.get_decimal_int(),
                   int_bytes, round_scale, out_scale, scaled_up_val))) {
      LOG_WARN("scale decimal int failed", K(ret), K(int_bytes), K(in_scale),
               K(out_scale), K(round_scale));
    } else if (OB_FAIL(ObDatumCast::align_decint_precision_unsafe(
      round_scale < out_scale ? scaled_up_val.get_decimal_int() : scaled_down_val.get_decimal_int(),
      round_scale < out_scale ? scaled_up_val.get_int_bytes() : scaled_down_val.get_int_bytes(),
      expected_int_bytes, res_val))) {
      LOG_WARN("align_decint_precision_unsafe failed", K(ret),
          K(scaled_down_val.get_int_bytes()), K(scaled_up_val.get_int_bytes()),
          K(expected_int_bytes), K(in_scale), K(out_scale), K(round_scale));
    }
  } else {
    res_val.from(decint, int_bytes);
  }
  return ret;
}

int ObExprFuncRound::calc_round_decimalint(
    const ObDatumMeta &in_meta, const ObDatumMeta &out_meta, const int64_t round_scale,
    const ObDatum &in_datum, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  if (in_meta.scale_ != round_scale
      || get_decimalint_type(in_meta.precision_) != get_decimalint_type(out_meta.precision_)) {
    ObDecimalIntBuilder res_val;
    if (OB_FAIL(do_round_decimalint(
        in_meta.precision_, in_meta.scale_, out_meta.precision_, out_meta.scale_, round_scale,
        in_datum, res_val))) {
      LOG_WARN("do_round_decimalint failed", K(ret), K(in_meta), K(out_meta), K(round_scale));
    } else {
      res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
    }
  } else {
    res_datum.set_decimal_int(in_datum.get_decimal_int(), in_datum.len_); // need deep copy
  }
  return ret;
}

template <typename LeftVec, typename ResVec>
int ObExprFuncRound::calc_round_decimalint(
    const ObDatumMeta &in_meta, const ObDatumMeta &out_meta, const int64_t round_scale,
    LeftVec *left_vec, ResVec *res_vec, const int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (in_meta.scale_ != round_scale
      || get_decimalint_type(in_meta.precision_) != get_decimalint_type(out_meta.precision_)) {
    ObDecimalIntBuilder res_val;
    ObDatum left_datum(left_vec->get_payload(idx), left_vec->get_length(idx), false);
    if (OB_FAIL(do_round_decimalint(
        in_meta.precision_, in_meta.scale_, out_meta.precision_, out_meta.scale_, round_scale,
        left_datum, res_val))) {
      LOG_WARN("do_round_decimalint failed", K(ret), K(in_meta), K(out_meta), K(round_scale));
    } else {
      res_vec->set_decimal_int(idx, res_val.get_decimal_int(), res_val.get_int_bytes());
    }
  } else {
    res_vec->set_decimal_int(idx, left_vec->get_decimal_int(idx), left_vec->get_length(idx));
  }
  return ret;
}

static int do_round_by_type(
    const ObDatumMeta &in_meta, const ObDatumMeta &out_meta, const int64_t round_scale,
    const ObDatum &x_datum, ObEvalCtx &ctx,
    ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  const ObObjType &x_type = in_meta.get_type();
  UNUSED(ctx);
  switch (x_type) {
    case ObNumberType:
    case ObUNumberType: {
      const number::ObNumber x_nmb(x_datum.get_number());
      number::ObNumber res_nmb;
      ObNumStackOnceAlloc tmp_alloc;
      if (OB_FAIL(res_nmb.from(x_nmb, tmp_alloc))) {
        LOG_WARN("get num from x failed", K(ret), K(x_nmb));
      } else if (OB_FAIL(res_nmb.round(GET_SCALE_FOR_CALC(round_scale)))) {
        LOG_WARN("eval round of res_nmb failed", K(ret), K(round_scale), K(res_nmb));
      } else {
        res_datum.set_number(res_nmb);
      }
      break;
    }
    case ObDecimalIntType: {
      if (OB_FAIL(ObExprFuncRound::calc_round_decimalint(
                  in_meta, out_meta, GET_SCALE_FOR_CALC(round_scale), x_datum, res_datum))) {
        LOG_WARN("calc_round_decimalint failed", K(ret), K(in_meta), K(out_meta), K(round_scale));
      }
      break;
    }
    case ObFloatType: {
      // if in Oracle mode, param_num must be 1(scale is 0)
      // MySQL mode cannot be here. because if param type is float, calc type will be double.
      res_datum.set_float(ObExprUtil::round_double(x_datum.get_float(), round_scale));
      break;
    }
    case ObDoubleType: {
      // if in Oracle mode, param_num must be 1(scale is 0)
      res_datum.set_double(ObExprUtil::round_double(x_datum.get_double(), round_scale));
      break;
    }
    case ObIntType: {
      int64_t x_int = x_datum.get_int();
      bool neg = x_int < 0;
      x_int = neg ? -x_int : x_int;
      int64_t res_int = static_cast<int64_t>(ObExprUtil::round_uint64(x_int, round_scale));
      res_int = neg ? -res_int : res_int;
      res_datum.set_int(res_int);
      break;
    }
    case ObUInt64Type: {
      uint64_t x_uint = x_datum.get_uint();
      uint64_t res_uint = ObExprUtil::round_uint64(x_uint, round_scale);
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
  bool is_need = ObBitVector::bit_op_zero(skip, eval_flags, batch_size,
                                      [](uint64_t l, uint64_t r) { return l | r; });
  for (int64_t i = 0; is_need && i < batch_size; ++i) {
    is_need = !(x_datums[i].is_null());
  }
  return is_need;
}

template <typename LeftVec>
static bool is_vector_need_cal_all(LeftVec *left_vec,
                            const ObBitVector &skip,
                            const ObBitVector &eval_flags,
                            const EvalBound &bound)
{
  bool is_need = ObBitVector::bit_op_zero(skip, eval_flags, bound,
                                      [](uint64_t l, uint64_t r) { return l | r; });
  for (int64_t j = bound.start(); is_need && j < bound.end(); ++j) {
    is_need = !(left_vec->is_null(j));
  }
  return is_need;
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
    case ObDecimalIntType: {
      const ObDatumMeta &in_meta = expr.args_[0]->datum_meta_;
      const ObDatumMeta &out_meta = expr.datum_meta_;
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        } else {
          ObDatum &x_datum = x_datums[i];
          eval_flags.set(i);
          if (x_datum.is_null()) {
            results[i].set_null();
          } else if (OB_FAIL(ObExprFuncRound::calc_round_decimalint(
                             in_meta, out_meta, GET_SCALE_FOR_CALC(scale), x_datum, results[i]))) {
            LOG_WARN("calc_round_decimalint failed", K(ret), K(in_meta), K(out_meta), K(scale));
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

#define CHECK_ROUND_VECTOR()               \
if (IsCheck) {                             \
  if (skip.at(j) || eval_flags.at(j)) {    \
    continue;                              \
  } else if (left_vec->is_null(j)) {       \
    res_vec->set_null(j);                  \
    eval_flags.set(j);                     \
    continue;                              \
  }                                        \
}

template <typename LeftVec, typename ResVec, bool IsCheck>
static int do_round_by_type_vector(const int64_t scale, const ObExpr &expr,
                                  ObEvalCtx &ctx, const ObBitVector &skip,
                                  const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (!IsCheck) { UNUSED(skip); }
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  LeftVec *left_vec = static_cast<LeftVec *>(expr.args_[0]->get_vector(ctx));
  const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
  switch (x_type) {
    case ObNumberType:
    case ObUNumberType: {
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
        CHECK_ROUND_VECTOR();
        const number::ObNumber x_nmb(left_vec->get_number(j));
        number::ObNumber res_nmb;
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_FAIL(res_nmb.from(x_nmb, tmp_alloc))) {
          LOG_WARN("get num from x_nmb failed", K(ret), K(x_nmb));
        } else if (OB_FAIL(res_nmb.round(GET_SCALE_FOR_CALC(scale)))) {
          LOG_WARN("eval round of res_nmb failed", K(ret), K(scale), K(res_nmb));
        } else {
          res_vec->set_number(j, res_nmb);
          eval_flags.set(j);
        }
      }
      break;
    }
    case ObDecimalIntType: {
      const ObDatumMeta &in_meta = expr.args_[0]->datum_meta_;
      const ObDatumMeta &out_meta = expr.datum_meta_;
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
        CHECK_ROUND_VECTOR();
        if (OB_FAIL((ObExprFuncRound::calc_round_decimalint<LeftVec, ResVec>)(
                            in_meta, out_meta, GET_SCALE_FOR_CALC(scale), left_vec, res_vec, j))) {
          LOG_WARN("calc_round_decimalint failed", K(ret), K(in_meta), K(out_meta), K(scale));
        }
        eval_flags.set(j);
      }
      break;
    }
    case ObFloatType: {
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
        CHECK_ROUND_VECTOR();
        // if in Oracle mode, param_num must be 1(scale is 0)
        // MySQL mode cannot be here. because if param type is float, calc type will be double.
        res_vec->set_float(j, ObExprUtil::round_double(left_vec->get_float(j), scale));
        eval_flags.set(j);
      }
      break;
    }
    case ObDoubleType: {
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
        CHECK_ROUND_VECTOR();
        // if in Oracle mode, param_num must be 1(scale is 0)
        res_vec->set_double(j, ObExprUtil::round_double(left_vec->get_double(j), scale));
        eval_flags.set(j);
      }
      break;
    }
    case ObIntType: {
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
        CHECK_ROUND_VECTOR();
        int64_t x_int = left_vec->get_int(j);
        bool neg = x_int < 0;
        x_int = neg ? -x_int : x_int;
        int64_t res_int = static_cast<int64_t>(ObExprUtil::round_uint64(x_int, scale));
        res_int = neg ? -res_int : res_int;
        res_vec->set_int(j, res_int);
        eval_flags.set(j);
      }
      break;
    }
    case ObUInt64Type: {
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
       CHECK_ROUND_VECTOR();
        uint64_t x_uint = left_vec->get_uint(j);
        uint64_t res_uint = ObExprUtil::round_uint64(x_uint, scale);
        res_vec->set_uint(j, res_uint);
        eval_flags.set(j);
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
    case ObDecimalIntType: {
      const ObDatumMeta &in_meta = expr.args_[0]->datum_meta_;
      const ObDatumMeta &out_meta = expr.datum_meta_;
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        if (OB_FAIL(ObExprFuncRound::calc_round_decimalint(
                    in_meta, out_meta, GET_SCALE_FOR_CALC(scale), x_datums[i], results[i]))) {
          LOG_WARN("calc_round_decimalint failed", K(ret), K(in_meta), K(out_meta), K(scale));
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
  if (OB_FAIL(expr.args_[0]->eval(ctx, x_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (x_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(do_round_by_type(
              expr.args_[0]->datum_meta_, expr.datum_meta_, 0, *x_datum, ctx, res_datum))) {
    LOG_WARN("calc round by type failed",
        K(ret), K(expr.args_[0]->datum_meta_), K(expr.datum_meta_));
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

#define ROUND_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(func_name, res_vec)            \
switch (left_format) {                                                          \
  case VEC_FIXED: {                                                             \
    ret = func_name<ObFixedLengthBase, res_vec>(expr, ctx, skip, bound);        \
    break;                                                                      \
  }                                                                             \
  case VEC_DISCRETE: {                                                          \
    ret = func_name<ObDiscreteFormat, res_vec>(expr, ctx, skip, bound);         \
    break;                                                                      \
  }                                                                             \
  case VEC_CONTINUOUS: {                                                        \
    ret = func_name<ObContinuousFormat, res_vec>(expr, ctx, skip, bound);       \
    break;                                                                      \
  }                                                                             \
  case VEC_UNIFORM: {                                                           \
    ret = func_name<ObUniformFormat<false>, res_vec>(expr, ctx, skip, bound);   \
    break;                                                                      \
  }                                                                             \
  case VEC_UNIFORM_CONST: {                                                     \
    ret = func_name<ObUniformFormat<true>, res_vec>(expr, ctx, skip, bound);    \
    break;                                                                      \
  }                                                                             \
  default: {                                                                    \
    ret = func_name<ObVectorBase, res_vec>(expr, ctx, skip, bound);             \
  }                                                                             \
}

#define ROUND_DISPATCH_VECTOR_IN_RES_ARG_FORMAT(func_name)                          \
switch (res_format) {                                                               \
  case VEC_FIXED: {                                                                 \
    ROUND_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(func_name, ObFixedLengthBase);         \
    break;                                                                          \
  }                                                                                 \
  case VEC_DISCRETE: {                                                              \
    ROUND_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(func_name, ObDiscreteFormat);          \
    break;                                                                          \
  }                                                                                 \
  case VEC_CONTINUOUS: {                                                            \
    ROUND_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(func_name, ObContinuousFormat);        \
    break;                                                                          \
  }                                                                                 \
  case VEC_UNIFORM: {                                                               \
    ROUND_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(func_name, ObUniformFormat<false>);    \
    break;                                                                          \
  }                                                                                 \
  case VEC_UNIFORM_CONST: {                                                         \
    ROUND_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(func_name, ObUniformFormat<true>);     \
    break;                                                                          \
  }                                                                                 \
  default: {                                                                        \
    ROUND_DISPATCH_VECTOR_IN_LEFT_ARG_FORMAT(func_name, ObVectorBase);              \
  }                                                                                 \
}

int ObExprFuncRound::calc_round_expr_numeric1_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else {
    VectorFormat res_format = expr.get_format(ctx);
    VectorFormat left_format = expr.args_[0]->get_format(ctx);
    ROUND_DISPATCH_VECTOR_IN_RES_ARG_FORMAT(inner_calc_round_expr_numeric1_vector);
  }
  return ret;
}

template <typename LeftVec, typename ResVec>
int ObExprFuncRound::inner_calc_round_expr_numeric1_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  LeftVec *left_vec = static_cast<LeftVec *>(expr.args_[0]->get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (is_vector_need_cal_all<LeftVec>(left_vec, skip, eval_flags, bound)) {
    if (OB_FAIL((do_round_by_type_vector<LeftVec, ResVec, false>)(0, expr, ctx, skip, bound))) {
      const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
      LOG_WARN("calc round by type failed", K(ret), K(x_type), K(expr));
    }
  } else {
    if (OB_FAIL((do_round_by_type_vector<LeftVec, ResVec, true>)(0, expr, ctx, skip, bound))) {
      const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
      LOG_WARN("calc round by type failed", K(ret), K(x_type), K(expr));
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
      if (is_mysql_mode()
          && (ob_is_number_tc(expr.args_[0]->datum_meta_.get_type())
              || ob_is_decimal_int_tc(expr.args_[0]->datum_meta_.get_type()))) {
        if (expr.args_[0]->datum_meta_.scale_ < scale
            // eg : select round(123.123, 100);
            //      -> result is 123.123
            || expr.datum_meta_.scale_ < scale) {
            // eg : select round(123.123456789123456789123456789123456789, 50);
            //      -> result accuracy is precision:34, scale:30 (max result scale is 30)
          scale = expr.datum_meta_.scale_;
        }
      }
      if (OB_FAIL(do_round_by_type(
                  expr.args_[0]->datum_meta_, expr.datum_meta_, scale, *x_datum, ctx, res_datum))) {
        LOG_WARN("calc round by type failed",
                 K(ret), K(expr.args_[0]->datum_meta_), K(expr.datum_meta_));
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
        if (is_mysql_mode()
            && (ob_is_number_tc(expr.args_[0]->datum_meta_.get_type())
                || ob_is_decimal_int_tc(expr.args_[0]->datum_meta_.get_type()))) {
          if (expr.args_[0]->datum_meta_.scale_ < scale
              // eg : select round(123.123, 100);
              //      -> result is 123.123
              || expr.datum_meta_.scale_ < scale) {
              // eg : select round(123.123456789123456789123456789123456789, 50);
              //      -> result accuracy is precision:34, scale:30 (max result scale is 30)
            scale = expr.datum_meta_.scale_;
          }
        }
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

int ObExprFuncRound::calc_round_expr_numeric2_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else {
    VectorFormat res_format = expr.get_format(ctx);
    VectorFormat left_format = expr.args_[0]->get_format(ctx);
    ROUND_DISPATCH_VECTOR_IN_RES_ARG_FORMAT(inner_calc_round_expr_numeric2_vector);
  }
  return ret;
}

template <typename LeftVec, typename ResVec>
int ObExprFuncRound::inner_calc_round_expr_numeric2_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObDatum *fmt_datum = NULL;
  if (OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
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
        ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
        for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
          eval_flags.set(j);
          res_vec->set_null(j);
        }
      } else {
        if (is_mysql_mode()
            && (ob_is_number_tc(expr.args_[0]->datum_meta_.get_type())
                || ob_is_decimal_int_tc(expr.args_[0]->datum_meta_.get_type()))) {
          if (expr.args_[0]->datum_meta_.scale_ < scale
              // eg : select round(123.123, 100);
              //      -> result is 123.123
              || expr.datum_meta_.scale_ < scale) {
              // eg : select round(123.123456789123456789123456789123456789, 50);
              //      -> result accuracy is precision:34, scale:30 (max result scale is 30)
            scale = expr.datum_meta_.scale_;
          }
        }
        LeftVec *left_vec = static_cast<LeftVec *>(expr.args_[0]->get_vector(ctx));
        ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
        if (is_vector_need_cal_all<LeftVec>(left_vec, skip, eval_flags, bound)) {
          if (OB_FAIL((do_round_by_type_vector<LeftVec, ResVec, false>)(scale, expr, ctx, skip, bound))) {
            const ObObjType x_type = expr.args_[0]->datum_meta_.type_;
            LOG_WARN("calc round by type failed", K(ret), K(x_type), K(expr));
          }
        } else {
          if (OB_FAIL((do_round_by_type_vector<LeftVec, ResVec, true>)(scale, expr, ctx, skip, bound))) {
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
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(x_datum, ObDateTimeType, NUMBER_SCALE_UNKNOWN_YET,
                              tz_info, ob_time,
                              get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), 0, false))) {
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

int ObExprFuncRound::calc_round_expr_datetime1_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval arg0 failed", K(ret), K(expr));
  } else {
    VectorFormat res_format = expr.get_format(ctx);
    VectorFormat left_format = expr.args_[0]->get_format(ctx);
    ROUND_DISPATCH_VECTOR_IN_RES_ARG_FORMAT(inner_calc_round_expr_datetime1_vector);
  }
  return ret;
}

template <typename LeftVec, typename ResVec>
int ObExprFuncRound::inner_calc_round_expr_datetime1_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObString fmt_str("DD");
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  LeftVec *left_vec = static_cast<LeftVec *>(expr.args_[0]->get_vector(ctx));
  for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
    if (skip.at(j) || eval_flags.at(j)) {
      continue;
    }
    int64_t dt = 0;
    if (left_vec->is_null(j)) {
      res_vec->set_null(j);
    } else {
      ObDatum left_datum(left_vec->get_payload(j), left_vec->get_length(j), false);
      if (OB_FAIL(calc_round_expr_datetime_inner(left_datum, fmt_str, ctx, dt, expr))) {
        LOG_WARN("calc_round_expr_datetime_inner failed", K(ret));
      } else {
        res_vec->set_datetime(j, dt);
      }
    }
    eval_flags.set(j);
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

int ObExprFuncRound::calc_round_expr_datetime2_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else {
    VectorFormat res_format = expr.get_format(ctx);
    VectorFormat left_format = expr.args_[0]->get_format(ctx);
    ROUND_DISPATCH_VECTOR_IN_RES_ARG_FORMAT(inner_calc_round_expr_datetime2_vector);
  }
  return ret;
}

template <typename LeftVec, typename ResVec>
int ObExprFuncRound::inner_calc_round_expr_datetime2_vector(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             const ObBitVector &skip,
                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDatum *fmt_datum = NULL;
  if (OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else {
    LeftVec *left_vec = static_cast<LeftVec *>(expr.args_[0]->get_vector(ctx));
    ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
    for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      int64_t dt = 0;
      eval_flags.set(j);
      if (left_vec->is_null(j) || fmt_datum->is_null()) {
        res_vec->set_null(j);
      } else {
        ObDatum left_datum(left_vec->get_payload(j), left_vec->get_length(j), false);
        if (OB_FAIL(calc_round_expr_datetime_inner(left_datum, fmt_datum->get_string(),
                                                  ctx, dt, expr))) {
          LOG_WARN("calc_round_expr_datetime_inner failed", K(ret));
        } else {
          res_vec->set_datetime(j, dt);
        }
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
            rt_expr.eval_vector_func_ = calc_round_expr_datetime2_vector;
          }
        } else {
          rt_expr.eval_func_ = calc_round_expr_numeric2;
          // Only implement vectorization when parameter 0 is batch and parameter 1 is constant
          if (rt_expr.args_[0]->is_batch_result() && !(rt_expr.args_[1]->is_batch_result())) {
            rt_expr.eval_batch_func_ = calc_round_expr_numeric2_batch;
            rt_expr.eval_vector_func_ = calc_round_expr_numeric2_vector;
          }
        }
      } else {
        rt_expr.eval_func_ = calc_round_expr_numeric2;
        // Only implement vectorization when parameter 0 is batch and parameter 1 is constant
        if (rt_expr.args_[0]->is_batch_result() && !(rt_expr.args_[1]->is_batch_result())) {
          rt_expr.eval_batch_func_ = calc_round_expr_numeric2_batch;
          rt_expr.eval_vector_func_ = calc_round_expr_numeric2_vector;
        }
      }
    } else {
      if (ObDateTimeType == x_type) {
        rt_expr.eval_func_ = calc_round_expr_datetime1;
        rt_expr.eval_batch_func_ = calc_round_expr_datetime1_batch;
        rt_expr.eval_vector_func_ = calc_round_expr_datetime1_vector;
      } else {
        rt_expr.eval_func_ = calc_round_expr_numeric1;
        rt_expr.eval_batch_func_ = calc_round_expr_numeric1_batch;
        rt_expr.eval_vector_func_ = calc_round_expr_numeric1_vector;
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprFuncRound, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(3);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_SQL_MODE);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_TIME_ZONE);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  } else {
    SET_LOCAL_SYSVAR_CAPACITY(5);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_NLS_DATE_FORMAT);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_NLS_TIMESTAMP_FORMAT);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_TIME_ZONE);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

} // sql
} // oceanbase
