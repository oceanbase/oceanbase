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

#include "sql/engine/expr/ob_expr_func_ceil.h"

#include <string.h>

#include "share/object/ob_obj_cast.h"

#include "sql/parser/ob_item_type.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace sql {
ObExprCeilFloor::ObExprCeilFloor(
    ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, param_num, dimension)
{}

ObExprCeilFloor::~ObExprCeilFloor()
{}

int ObExprCeilFloor::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
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
      if (!type1.is_oracle_decimal()) {
        type1.set_calc_type(ObNumberType);
      } else {
        type1.set_calc_type(type1.get_type());
        if (ObNumberTC == ob_obj_type_class(type1.get_calc_type())) {
          type1.set_calc_accuracy(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObNumberType]);
        }
      }
    }
  } else {
    if (type1.is_column() && type1.is_number()) {
      type.set_number();
      int16_t prec = type1.get_precision();
      int16_t scale = type1.get_scale();
      type.set_number();
      // to be compatible with mysql.
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
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected result type", K(ret), K(type1), K(res_type));
      } else {
        type.set_type(res_type);
        const ObSQLSessionInfo* session = dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
        if (OB_ISNULL(session)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cast basic session to sql session info failed", K(ret));
        } else {
          type1.set_calc_type(res_type);
        }
      }
    }
    // no need to test ret here
    // scale
    type.set_scale(0);
    type.set_precision(type1.get_precision());
  }

  ObExprOperator::calc_result_flag1(type, type1);
  return ret;
}

int ObExprCeilFloor::calc_result1(ObObj& result, const ObObj& input, ObExprCtx& expr_ctx, bool is_ceil) const
{
  int ret = OB_SUCCESS;
  if (NULL == expr_ctx.calc_buf_) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (input.is_null()) {
    result.set_null();
  } else {
    ObObjType type = input.get_type();
    if (is_oracle_mode()) {
      if (input.is_float()) {
        if (is_ceil) {
          result.set_float(ceilf(input.get_float()));
        } else {
          result.set_float(floorf(input.get_float()));
        }
      } else if (input.is_double()) {
        if (is_ceil) {
          result.set_double(ceil(input.get_double()));
        } else {
          result.set_double(floor(input.get_double()));
        }
      } else if (input.is_number()) {
        number::ObNumber tmp_number;
        int64_t scale = 0;
        if (OB_FAIL(tmp_number.from(input.get_number(), *expr_ctx.calc_buf_))) {
          LOG_WARN("copy number fail", K(ret));
        } else if (OB_FAIL(is_ceil ? tmp_number.ceil(scale) : tmp_number.floor(scale))) {
          LOG_WARN("get ceil/floor(number) failed", K(is_ceil), K(ret));
        } else {
          result.set_number(tmp_number);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(input), K(ret));
      }
      LOG_DEBUG("finish ceil/floor", K(is_ceil), K(input), K(result), K(ret));
    } else {
      ObObj input_copy = input;
      if (type != result_type_.get_type() && !(ObNumberType == type && ObIntType == result_type_.get_type())) {
        ObCastMode cast_mode = get_cast_mode();
        EXPR_DEFINE_CAST_CTX(expr_ctx, cast_mode);
        cast_ctx.expect_obj_collation_ = cast_ctx.dest_collation_;
        if (OB_FAIL(ObObjCaster::to_type(result_type_.get_type(), cast_ctx, input, input_copy))) {
          LOG_WARN("fail to convert type", K(ret), K(result_type_), K(input));
        }
      }
      type = input_copy.get_type();
      if (OB_SUCC(ret)) {
        if (ob_is_int_tc(type)) {
          result.set_int(input_copy.get_int());
        } else if (ob_is_uint_tc(type)) {
          result.set_uint64(input_copy.get_uint64());
        } else if (ob_is_double_tc(type)) {
          result.set_double(is_ceil ? ceil(input_copy.get_double()) : floor(input_copy.get_double()));
        } else if (ob_is_number_tc(type)) {
          number::ObNumber num = input_copy.get_number();
          number::ObNumber tmp_number;
          int64_t scale = 0;
          if (OB_FAIL(tmp_number.from(num, *expr_ctx.calc_buf_))) {
            LOG_WARN("copy number fail", K(ret));
          } else if (OB_FAIL(is_ceil ? tmp_number.ceil(scale) : tmp_number.floor(scale))) {
            LOG_WARN("get ceil/floor(number) failed", K(is_ceil), K(ret));
          } else if (ob_is_number_tc(result_type_.get_type())) {
            result.set_type(result_type_.get_type());
            result.set_number_value(tmp_number);
          } else {
            int64_t res_int = 0;
            if (tmp_number.is_valid_int64(res_int)) {
              result.set_int(res_int);
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected input number value", K(ret), K(input_copy), K(result_type_));
            }
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid input type", K(ret), K(type));
        }
      }
    }
  }
  return ret;
}

int calc_ceil_floor(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg_datum = NULL;
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

int ObExprCeilFloor::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_ceil_floor;
  return ret;
}

// func ceil()
ObExprFuncCeil::ObExprFuncCeil(ObIAllocator& alloc)
    : ObExprCeilFloor(alloc, T_FUN_SYS_CEIL, "ceil", 1, NOT_ROW_DIMENSION)
{}

ObExprFuncCeil::~ObExprFuncCeil()
{}

int ObExprFuncCeil::calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  return ObExprCeilFloor::calc_result_type1(type, type1, type_ctx);
}

int ObExprFuncCeil::calc_result1(ObObj& result, const ObObj& input, ObExprCtx& expr_ctx) const
{
  return ObExprCeilFloor::calc_result1(result, input, expr_ctx, true);
}

// func ceiling()
ObExprFuncCeiling::ObExprFuncCeiling(ObIAllocator& alloc)
    : ObExprCeilFloor(alloc, T_FUN_SYS_CEILING, "ceiling", 1, NOT_ROW_DIMENSION)
{}

ObExprFuncCeiling::~ObExprFuncCeiling()
{}

int ObExprFuncCeiling::calc_result_type1(
    ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  return ObExprCeilFloor::calc_result_type1(type, type1, type_ctx);
}

int ObExprFuncCeiling::calc_result1(ObObj& result, const ObObj& input, ObExprCtx& expr_ctx) const
{
  return ObExprCeilFloor::calc_result1(result, input, expr_ctx, true);
}

ObExprFuncFloor::ObExprFuncFloor(ObIAllocator& alloc)
    : ObExprCeilFloor(alloc, T_FUN_SYS_FLOOR, "floor", 1, NOT_ROW_DIMENSION)
{}

// func floor()
ObExprFuncFloor::~ObExprFuncFloor()
{}

int ObExprFuncFloor::calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  return ObExprCeilFloor::calc_result_type1(type, type1, type_ctx);
}

int ObExprFuncFloor::calc_result1(ObObj& result, const ObObj& input, ObExprCtx& expr_ctx) const
{
  return ObExprCeilFloor::calc_result1(result, input, expr_ctx, false);
}

}  // namespace sql
}  // namespace oceanbase
