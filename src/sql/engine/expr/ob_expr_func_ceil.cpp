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

#include "sql/engine/expr/ob_expr_func_ceil.h"

#include <string.h>

#include "share/object/ob_obj_cast.h"

#include "objit/common/ob_item_type.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_truncate.h"
#include "sql/engine/expr/ob_expr_func_round.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObExprCeilFloor::ObExprCeilFloor(ObIAllocator &alloc,
                                 ObExprOperatorType type,
                                 const char *name,
                                 int32_t param_num,
                                 int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprCeilFloor::~ObExprCeilFloor()
{
}

int ObExprCeilFloor::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjType res_type = ObMaxType;
  if (is_oracle_mode()) {
    if (OB_FAIL(ObExprResultTypeUtil::get_round_result_type(res_type, type1.get_type()))) {
      LOG_WARN("get round result type failed", K(ret), K(type1), K(res_type));
    } else {
      type.set_type(res_type);
      type.set_scale(NUMBER_SCALE_UNKNOWN_YET);
      type.set_precision(PRECISION_UNKNOWN_YET);
      type.set_calc_scale(0);
      // select ceil(3.1) as a from dual
      // '3.1' parsed as decimal int
      // result type of `ceil` function is ObNumber, therefore we need to add cast expr for decimal int param
      //
      // TODO: use decimal int as arg & result type instread.
      if (!type1.is_oracle_decimal() || type1.is_decimal_int()) {
        type1.set_calc_type(ObNumberType);
      } else {
        type1.set_calc_type(type1.get_type());
        if (ObNumberTC == ob_obj_type_class(type1.get_calc_type())) {
          type1.set_calc_accuracy(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObNumberType]);
        }
      }
    }
  } else {
    if (type1.is_column()
        && (type1.is_number() || type1.is_decimal_int())) {
      if (type1.is_number()) {
        type.set_number();
      } else {
        type.set_decimal_int(0);
      }
      int16_t prec = type1.get_precision();
      int16_t scale = type1.get_scale();
      //to be compatible with mysql.
      if (scale > 0) {
        if (prec - scale <= MAX_LIMIT_WITH_SCALE)
        type.set_int();
      } else if (prec <= MAX_LIMIT_WITHOUT_SCALE) {
        type.set_int();
      }
    } else {
      if (OB_FAIL(ObExprResultTypeUtil::get_round_result_type(res_type, type1.get_type()))) {
        LOG_WARN("get round result type failed", K(ret), K(type1), K(res_type));
      } else if (ObMaxType == res_type) {
        // 兼容mysql处理不合法类型的报错行为
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("unexpected result type", K(ret), K(type1), K(res_type));
      } else {
        type.set_type(res_type);
        type1.set_calc_type(res_type);
      }
    }
    //no need to test ret here
    // scale
    type.set_scale(0);
    if (lib::is_mysql_mode() && type.is_double()) {
      type.set_precision(17); // float length of 0
    } else {
      type.set_precision(type1.get_precision());
    }
  }

  ObExprOperator::calc_result_flag1(type, type1);
  return ret;
}

#define DECIMAL_INT_MOD(TYPE)                                        \
  case sizeof(TYPE##_t): {                                           \
    const TYPE##_t &l = *(decint->TYPE##_v_);                        \
    const TYPE##_t r = get_scale_factor<TYPE##_t>(trunc_scale);      \
    is_all_zero_truncated = ((l % r) == 0);                          \
    break;                                                           \
  }                                                                  \

#define DECIMAL_INT_INC(TYPE)                                        \
  case sizeof(TYPE##_t) : {                                          \
    TYPE##_t tmp_num = *(res_val.get_decimal_int()->TYPE##_v_);      \
    res_val.from(++tmp_num);                                         \
    break;                                                           \
  }                                                                  \

#define DECIMAL_INT_DEC(TYPE)                                        \
  case sizeof(TYPE##_t) : {                                          \
    TYPE##_t tmp_num = *(res_val.get_decimal_int()->TYPE##_v_);      \
    res_val.from(--tmp_num);                                         \
    break;                                                           \
  }                                                                  \

int ObExprCeilFloor::ceil_floor_decint(
    const bool is_floor, const ObDatum *arg_datum,
    const ObDatumMeta &in_meta, const ObDatumMeta &out_meta,
    ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  const ObDecimalInt *decint = arg_datum->get_decimal_int();
  const int32_t int_bytes = arg_datum->get_int_bytes();
  const bool is_neg = wide::is_negative(decint, int_bytes);
  ObDecimalIntBuilder res_val;
  if (in_meta.scale_ < out_meta.scale_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in_scale_ < out_scale is unexpected", K(ret), K(in_meta.scale_), K(out_meta.scale_));
  } else if (in_meta.scale_ == out_meta.scale_) {
    res_val.from(decint, int_bytes);
  } else if (OB_FAIL(ObExprTruncate::do_trunc_decimalint(in_meta.precision_, in_meta.scale_,
              out_meta.precision_, out_meta.scale_, out_meta.scale_, *arg_datum, res_val))) {
    LOG_WARN("calc_trunc_decimalint failed", K(ret), K(in_meta.precision_), K(in_meta.scale_),
             K(out_meta.precision_), K(out_meta.scale_));
  } else if ((is_floor && !is_neg) || (!is_floor && is_neg)) {
    // truncate to scale 0 directly when we floor a pos or ceil a neg, do nothing
  } else {
    int16_t trunc_scale = in_meta.scale_ - out_meta.scale_;
    bool is_all_zero_truncated = true; // equal to is_integer while out_scale = 0
    switch (int_bytes) {
      DECIMAL_INT_MOD(int32)
      DECIMAL_INT_MOD(int64)
      DECIMAL_INT_MOD(int128)
      DECIMAL_INT_MOD(int256)
      DECIMAL_INT_MOD(int512)
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("int_bytes is unexpected", K(ret), K(int_bytes));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (!is_all_zero_truncated) { // arg is not integer
        if (is_floor && is_neg) { // floor a neg, dec after trunc
          switch (res_val.get_int_bytes()) {
            DECIMAL_INT_DEC(int32)
            DECIMAL_INT_DEC(int64)
            DECIMAL_INT_DEC(int128)
            DECIMAL_INT_DEC(int256)
            DECIMAL_INT_DEC(int512)
            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("int_bytes is unexpected", K(ret), K(int_bytes));
              break;
            }
          }
        } else if (!is_floor && !is_neg) { // ceil a pos, inc after trunc
          switch (res_val.get_int_bytes()) {
            DECIMAL_INT_INC(int32)
            DECIMAL_INT_INC(int64)
            DECIMAL_INT_INC(int128)
            DECIMAL_INT_INC(int256)
            DECIMAL_INT_INC(int512)
            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("int_bytes is unexpected", K(ret), K(int_bytes));
              break;
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ob_is_decimal_int(out_meta.type_)) {
      res_datum.set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
    } else if (ob_is_integer_type(out_meta.type_)) {
      int64_t res_int = 0;
      bool is_valid_int64 = true;
      if (OB_FAIL(wide::check_range_valid_int64(
          res_val.get_decimal_int(), res_val.get_int_bytes(), is_valid_int64, res_int))) {
        LOG_WARN("check_range_valid_int64 failed", K(ret), K(res_val.get_int_bytes()));
      } else if (is_valid_int64) {
        res_datum.set_int(res_int);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected res type", K(ret), K(out_meta.type_));
      }
    }
  }
  return ret;
}
#undef DECIMAL_INT_MOD
#undef DECIMAL_INT_INC
#undef DECIMAL_INT_DEC

int calc_ceil_floor(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg_datum))) {
    LOG_WARN("eval arg 0 failed", K(ret), K(expr));
  } else if (arg_datum->is_null()) {
    res_datum.set_null();
  } else {
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    const ObObjType res_type = expr.datum_meta_.type_;
    const bool is_floor = (T_FUN_SYS_FLOOR == expr.type_) ? true : false;
    if (ObNumberTC == ob_obj_type_class(arg_type)) {
      ObNumStackOnceAlloc tmp_alloc;
      const number::ObNumber arg_nmb(arg_datum->get_number());
      number::ObNumber res_nmb;
      if (OB_FAIL(res_nmb.from(arg_nmb, tmp_alloc))) {
        LOG_WARN("get number from arg failed", K(ret), K(arg_nmb));
      } else {
        if (is_floor) {
          if (OB_FAIL(res_nmb.floor(0))) {
            LOG_WARN("calc floor for number failed", K(ret), K(res_nmb));
          }
        } else {
          if (OB_FAIL(res_nmb.ceil(0))) {
            LOG_WARN("calc ceil for number failed", K(ret), K(res_nmb));
          }
        }
        if (OB_SUCC(ret)) {
          if (ObNumberTC == ob_obj_type_class(res_type)) {
            res_datum.set_number(res_nmb);
          } else if (ob_is_integer_type(res_type)) {
            int64_t res_int = 0;
            if (res_nmb.is_valid_int64(res_int)) {
              res_datum.set_int(res_int);
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected res type", K(ret), K(res_type));
            }
          }
        }
      }
    } else if (ob_is_decimal_int(arg_type)) {
      const ObDatumMeta &in_meta = expr.args_[0]->datum_meta_;
      const ObDatumMeta &out_meta = expr.datum_meta_;
      if (OB_FAIL(ObExprCeilFloor::ceil_floor_decint(
                  is_floor, arg_datum, in_meta, out_meta, res_datum))) {
        LOG_WARN("ceil_floor_decint failed", K(ret), K(is_floor), K(in_meta), K(out_meta));
      }
    } else if (ob_is_integer_type(arg_type) && ob_is_integer_type(res_type)) {
      res_datum.set_int(arg_datum->get_int());
    } else if (ObFloatType == arg_type && ObFloatType == res_type) {
      if (is_floor) {
        res_datum.set_float(floorf(arg_datum->get_float()));
      } else {
        res_datum.set_float(ceilf(arg_datum->get_float()));
      }
    } else if (ObDoubleType == arg_type && ObDoubleType == res_type) {
      if (is_floor) {
        res_datum.set_double(floor(arg_datum->get_double()));
      } else {
        res_datum.set_double(ceil(arg_datum->get_double()));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected res type or arg type", K(ret), K(res_type), K(arg_type));
    }
  }
  return ret;
}

#define NUMBER_TYPE       0
#define INTEGER_TYPE      1
#define FLOAT_TYPE        2
#define DOUBLE_TYPE       3
#define DECIMAL_INT_TYPE  4

template <int TYPE, bool IS_FLOOR>
int do_eval_batch_ceil_floor(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  static_assert(TYPE >= 0 && TYPE <= 4, "TYPE value out of range");
  ObBitVector &eval_flag = expr.get_evaluated_flags(ctx);
  ObDatumVector arg_datums = expr.args_[0]->locate_expr_datumvector(ctx);
  ObDatum *res_datums = expr.locate_batch_datums(ctx);
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; i++) {
    if (!eval_flag.contain(i) && !skip.contain(i)) {
      if (NUMBER_TYPE == TYPE) {
        if (arg_datums.at(i)->is_null()) {
          res_datums[i].set_null();
        } else {
          number::ObNumber arg_nmb(arg_datums.at(i)->get_number());
          ObNumStackOnceAlloc tmp_alloc;
          number::ObNumber res_nmb;
          if (OB_FAIL(res_nmb.from(arg_nmb, tmp_alloc))) {
            LOG_WARN("get number from arg failed", K(ret), K(arg_nmb));
          } else {
            if (IS_FLOOR) {
              if (OB_FAIL(res_nmb.floor(0))) {
                LOG_WARN("calc floor for number failed", K(ret), K(res_nmb));
              }
            } else {
              if (OB_FAIL(res_nmb.ceil(0))) {
                LOG_WARN("calc ceil for number failed", K(ret), K(res_nmb));
              }
            }
            const ObObjType res_type = expr.datum_meta_.type_;
            if (ObNumberTC == ob_obj_type_class(res_type)) {
              res_datums[i].set_number(res_nmb);
            } else if (ob_is_integer_type(res_type)) {
              int64_t res_int = 0;
              if (res_nmb.is_valid_int64(res_int)) {
                res_datums[i].set_int(res_int);
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected res type", K(ret), K(res_type));
              }
            }
          }
        }
      } else if (INTEGER_TYPE == TYPE) {
        if (arg_datums.at(i)->is_null()) {
          res_datums[i].set_null();
        } else {
          res_datums[i].set_int(arg_datums.at(i)->get_int());
        }
      } else if (FLOAT_TYPE == TYPE) {
        if (arg_datums.at(i)->is_null()) {
          res_datums[i].set_null();
        } else if (IS_FLOOR) {
          res_datums[i].set_float(floorf(arg_datums.at(i)->get_float()));
        } else {
          res_datums[i].set_float(ceilf(arg_datums.at(i)->get_float()));
        }
      } else if (DOUBLE_TYPE == TYPE) {
        if (arg_datums.at(i)->is_null()) {
          res_datums[i].set_null();
        } else if (IS_FLOOR) {
          res_datums[i].set_double(floor(arg_datums.at(i)->get_double()));
        } else {
          res_datums[i].set_double(ceil(arg_datums.at(i)->get_double()));
        }
      } else if (DECIMAL_INT_TYPE == TYPE) {
        const ObDatumMeta &in_meta = expr.args_[0]->datum_meta_;
        const ObDatumMeta &out_meta = expr.datum_meta_;
        if (arg_datums.at(i)->is_null()) {
          res_datums[i].set_null();
        } else if (OB_FAIL(ObExprCeilFloor::ceil_floor_decint(
                           IS_FLOOR, arg_datums.at(i), in_meta, out_meta, res_datums[i]))) {
          LOG_WARN("ceil_floor_decint failed",
                K(ret), K(IS_FLOOR), K(in_meta), K(out_meta), K(arg_datums.at(i)->get_int_bytes()));
        }
      }
      eval_flag.set(i);
    }
  }
  return ret;
}

int eval_batch_ceil_floor(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          const ObBitVector &skip,
                          const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("vectorized evaluate failed", K(ret), K(expr));
  } else {
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    const bool is_floor = (T_FUN_SYS_FLOOR == expr.type_) ? true : false;
    if (ObNumberTC == ob_obj_type_class(arg_type)) {
      ret = is_floor
      ? do_eval_batch_ceil_floor<NUMBER_TYPE, true>(expr, ctx, skip, batch_size)
      : do_eval_batch_ceil_floor<NUMBER_TYPE, false>(expr, ctx, skip, batch_size);
    } else if (ob_is_integer_type(arg_type)) {
      ret = do_eval_batch_ceil_floor<INTEGER_TYPE, true>(expr, ctx, skip, batch_size);
    } else if (ObFloatType == arg_type) {
      ret = is_floor
      ? do_eval_batch_ceil_floor<FLOAT_TYPE, true>(expr, ctx, skip, batch_size)
      : do_eval_batch_ceil_floor<FLOAT_TYPE, false>(expr, ctx, skip, batch_size);
    } else if (ObDoubleType == arg_type) {
      ret = is_floor
      ? do_eval_batch_ceil_floor<DOUBLE_TYPE, true>(expr, ctx, skip, batch_size)
      : do_eval_batch_ceil_floor<DOUBLE_TYPE, false>(expr, ctx, skip, batch_size);
    } else if (ObDecimalIntType == arg_type) {
      ret = is_floor
      ? do_eval_batch_ceil_floor<DECIMAL_INT_TYPE, true>(expr, ctx, skip, batch_size)
      : do_eval_batch_ceil_floor<DECIMAL_INT_TYPE, false>(expr, ctx, skip, batch_size);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg type", K(ret), K(arg_type));
    }
  }
  return ret;
}

int ObExprCeilFloor::calc_ceil_floor_vector(const ObExpr &expr,
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
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    const ObObjType res_type = expr.datum_meta_.type_;
    int input_type = 0;
    if (ObNumberTC == ob_obj_type_class(arg_type)) {
      input_type = NUMBER_TYPE;
    } else if (ob_is_integer_type(arg_type)) {
      input_type = INTEGER_TYPE;
    } else if (ObFloatType == arg_type) {
      input_type = FLOAT_TYPE;
    } else if (ObDoubleType == arg_type) {
      input_type = DOUBLE_TYPE;
    } else if (ObDecimalIntType == arg_type) {
      input_type = DECIMAL_INT_TYPE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg type", K(ret), K(arg_type));
    }
    VecValueTypeClass arg_tc = get_vec_value_tc(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.scale_,
              expr.args_[0]->datum_meta_.precision_);
    if (OB_SUCC(ret)) {
      if (res_format == VEC_FIXED && left_format == VEC_FIXED) {
        if (arg_tc == VEC_TC_INTEGER || arg_tc == VEC_TC_UINTEGER) {
          ret = inner_calc_ceil_floor_vector<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>, ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>>
                                (expr, input_type, ctx, skip, bound);
        } else if (arg_tc == VEC_TC_DOUBLE || arg_tc == VEC_TC_FIXED_DOUBLE) {
          ret = inner_calc_ceil_floor_vector<ObFixedLengthFormat<RTCType<VEC_TC_DOUBLE>>, ObFixedLengthFormat<RTCType<VEC_TC_DOUBLE>>>
                      (expr, input_type, ctx, skip, bound);
        } else if (ObDecimalIntType == arg_type) {
          // if use ObFixedLengthFormat, don't know length of decimal_int
          // use ObFixedLengthBase, don't need length
          ret = inner_calc_ceil_floor_vector<ObFixedLengthBase, ObFixedLengthBase>(expr, input_type, ctx, skip, bound);
        } else {
          ret = inner_calc_ceil_floor_vector<ObVectorBase, ObVectorBase>(expr, input_type, ctx, skip, bound);
        }
      } else {
        // float type input is uniform....., output is fixed format...
        ret = inner_calc_ceil_floor_vector<ObVectorBase, ObVectorBase>(expr, input_type, ctx, skip, bound);
      }
    }
  }
  return ret;
}

#define CHECK_CEIL_VECTOR()               \
if (IsCheck) {                             \
  if (skip.at(j) || eval_flags.at(j)) {    \
    continue;                              \
  } else if (left_vec->is_null(j)) {       \
    res_vec->set_null(j);                  \
    eval_flags.set(j);                     \
    continue;                              \
  }                                        \
}

template <typename LeftVec, typename ResVec, bool IS_FLOOR, bool IsCheck>
static int do_eval_ceil_floor_vector(const ObExpr &expr,ObEvalCtx &ctx,
                                     int input_type,
                                     const ObBitVector &skip,
                                     const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  LeftVec *left_vec = static_cast<LeftVec *>(expr.args_[0]->get_vector(ctx));

  switch (input_type) {
    case NUMBER_TYPE: {
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
        CHECK_CEIL_VECTOR();
        const number::ObNumber arg_nmb(left_vec->get_number(j));
        number::ObNumber res_nmb;
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_FAIL(res_nmb.from(arg_nmb, tmp_alloc))) {
          LOG_WARN("get number from arg failed", K(ret), K(arg_nmb));
        } else {
          if (IS_FLOOR) {
            if (OB_FAIL(res_nmb.floor(0))) {
              LOG_WARN("calc floor for number failed", K(ret), K(res_nmb));
            }
          } else {
            if (OB_FAIL(res_nmb.ceil(0))) {
              LOG_WARN("calc ceil for number failed", K(ret), K(res_nmb));
            }
          }
          const ObObjType res_type = expr.datum_meta_.type_;
          if (OB_FAIL(ret)) {
          } else if (ObNumberTC == ob_obj_type_class(res_type)) {
            res_vec->set_number(j, res_nmb);
            eval_flags.set(j);
          } else if (ob_is_integer_type(res_type)) {
            int64_t res_int = 0;
            if (res_nmb.is_valid_int64(res_int)) {
              res_vec->set_int(j, res_int);
              eval_flags.set(j);
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected res type", K(ret), K(res_type));
            }
          }
        }
      }
      break;
    }
    case INTEGER_TYPE: {
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
        CHECK_CEIL_VECTOR();
        res_vec->set_int(j, left_vec->get_int(j));
        eval_flags.set(j);
      }
      break;
    }
    case FLOAT_TYPE: {
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
        CHECK_CEIL_VECTOR();
        if (IS_FLOOR) {
          res_vec->set_float(j, floorf(left_vec->get_float(j)));
        } else {
          res_vec->set_float(j, ceilf(left_vec->get_float(j)));
        }
        eval_flags.set(j);
      }
      break;
    }
    case DOUBLE_TYPE: {
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
        CHECK_CEIL_VECTOR();
        if (IS_FLOOR) {
          res_vec->set_double(j, floor(left_vec->get_double(j)));
        } else {
          res_vec->set_double(j, ceil(left_vec->get_double(j)));
        }
        eval_flags.set(j);
      }
      break;
    }
    case DECIMAL_INT_TYPE: {
      const ObDatumMeta &in_meta = expr.args_[0]->datum_meta_;
      const ObDatumMeta &out_meta = expr.datum_meta_;
      for (int64_t j = bound.start(); OB_SUCC(ret) && j < bound.end(); ++j) {
        CHECK_CEIL_VECTOR();
        ObDatum left_datum(left_vec->get_payload(j), left_vec->get_length(j), false);
        ObDatum res_datum(res_vec->get_payload(j), res_vec->get_length(j), false);
        if (OB_FAIL(ObExprCeilFloor::ceil_floor_decint(IS_FLOOR, &left_datum, in_meta, out_meta, res_datum))) {
          LOG_WARN("do_round_decimalint failed", K(ret), K(in_meta), K(out_meta), K(left_datum), K(res_datum));
        }
        res_vec->set_decimal_int(j, res_datum.get_decimal_int(), res_datum.get_int_bytes());
        eval_flags.set(j);
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg type", K(ret), K(input_type));
      break;
  }
  return ret;
}

uint64_t bitwise_or(uint64_t l, uint64_t r) {
    return l | r;
}

template <typename LeftVec>
static bool if_vector_need_cal_all(LeftVec *left_vec,
                            const ObBitVector &skip,
                            const ObBitVector &eval_flags,
                            const EvalBound &bound)
{
  bool is_need = ObBitVector::bit_op_zero(skip, eval_flags, bound, bitwise_or);
  for (int64_t j = bound.start(); is_need && j < bound.end(); ++j) {
    is_need = !(left_vec->is_null(j));
  }
  return is_need;
}

template <typename LeftVec, typename ResVec>
int ObExprCeilFloor::inner_calc_ceil_floor_vector(const ObExpr &expr,
                                                 int input_type,
                                                 ObEvalCtx &ctx,
                                                 const ObBitVector &skip,
                                                 const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  LeftVec *left_vec = static_cast<LeftVec *>(expr.args_[0]->get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx); // to do for skip
  const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
  const bool is_floor = (T_FUN_SYS_FLOOR == expr.type_) ? true : false;
  const bool is_check = if_vector_need_cal_all<LeftVec>(left_vec, skip, eval_flags, bound) ? false : true;

  if (is_check && is_floor) {
    ret = do_eval_ceil_floor_vector<LeftVec, ResVec, true, true>(expr, ctx, input_type, skip, bound);
  } else if (!is_check && is_floor) {
    ret = do_eval_ceil_floor_vector<LeftVec, ResVec, true, false>(expr, ctx, input_type, skip, bound);
  } else if (is_check && !is_floor) {
    ret = do_eval_ceil_floor_vector<LeftVec, ResVec, false, true>(expr, ctx, input_type, skip, bound);
  } else if (!is_check && !is_floor) {
    ret = do_eval_ceil_floor_vector<LeftVec, ResVec, false, false>(expr, ctx, input_type, skip, bound);
  }
  return ret;
}

int ObExprCeilFloor::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_ceil_floor;
  rt_expr.eval_batch_func_ = eval_batch_ceil_floor;
  rt_expr.eval_vector_func_ = calc_ceil_floor_vector;
  return ret;
}

//func ceil()
ObExprFuncCeil::ObExprFuncCeil(ObIAllocator &alloc)
    : ObExprCeilFloor(alloc, T_FUN_SYS_CEIL, "ceil", 1, NOT_ROW_DIMENSION)
{
}

ObExprFuncCeil::~ObExprFuncCeil()
{
}

int ObExprFuncCeil::calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const
{
  return ObExprCeilFloor::calc_result_type1(type, type1, type_ctx);
}

// func ceiling()
ObExprFuncCeiling::ObExprFuncCeiling(ObIAllocator &alloc)
    : ObExprCeilFloor(alloc, T_FUN_SYS_CEILING, "ceiling", 1, NOT_ROW_DIMENSION)
{
}

ObExprFuncCeiling::~ObExprFuncCeiling()
{
}

int ObExprFuncCeiling::calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const
{
  return ObExprCeilFloor::calc_result_type1(type, type1, type_ctx);
}

ObExprFuncFloor::ObExprFuncFloor(ObIAllocator &alloc)
    : ObExprCeilFloor(alloc, T_FUN_SYS_FLOOR, "floor", 1, NOT_ROW_DIMENSION)
{
}

// func floor()
ObExprFuncFloor::~ObExprFuncFloor()
{
}

int ObExprFuncFloor::calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const
{
  return ObExprCeilFloor::calc_result_type1(type, type1, type_ctx);
}

} // namespace sql
} // namespace oceanbase
