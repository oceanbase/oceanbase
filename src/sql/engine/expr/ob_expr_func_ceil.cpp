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
        const ObSQLSessionInfo *session =
          dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
        if (OB_ISNULL(session)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cast basic session to sql session info failed", K(ret));
        } else {
          type1.set_calc_type(res_type);
        }
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

//  TYPE         value 
//  ObNumberTC      0
//  ObIntegerType   1
//  ObFloatType     2
//  ObDoubleType    3
template <int TYPE, bool IS_FLOOR>
int do_eval_batch_ceil_floor(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            const ObBitVector &skip,
                            const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  static_assert(TYPE >= 0 && TYPE <= 3, "TYPE value out of range");
  ObBitVector &eval_flag = expr.get_evaluated_flags(ctx);
  ObDatumVector arg_datums = expr.args_[0]->locate_expr_datumvector(ctx);
  ObDatum *res_datums = expr.locate_batch_datums(ctx);
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; i++) {
    if (!eval_flag.contain(i) && !skip.contain(i)) {
      if (0 == TYPE) {
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
      } else if (1 == TYPE) {
        if (arg_datums.at(i)->is_null()) {
          res_datums[i].set_null();
        } else {
          res_datums[i].set_int(arg_datums.at(i)->get_int());
        }
      } else if (2 == TYPE) {
        if (arg_datums.at(i)->is_null()) {
          res_datums[i].set_null();
        } else if (IS_FLOOR) {
          res_datums[i].set_float(floorf(arg_datums.at(i)->get_float()));
        } else {
          res_datums[i].set_float(ceilf(arg_datums.at(i)->get_float()));
        }
      } else if (3 == TYPE) {
        if (arg_datums.at(i)->is_null()) {
          res_datums[i].set_null();
        } else if (IS_FLOOR) {
          res_datums[i].set_double(floor(arg_datums.at(i)->get_double()));
        } else {
          res_datums[i].set_double(ceil(arg_datums.at(i)->get_double()));
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
      ? do_eval_batch_ceil_floor<0,true>(expr, ctx, skip, batch_size)
      : do_eval_batch_ceil_floor<0,false>(expr, ctx, skip, batch_size);
    } else if (ob_is_integer_type(arg_type)) {
      ret = do_eval_batch_ceil_floor<1,true>(expr, ctx, skip, batch_size);
    } else if (ObFloatType == arg_type) {
      ret = is_floor
      ? do_eval_batch_ceil_floor<2,true>(expr, ctx, skip, batch_size)
      : do_eval_batch_ceil_floor<2,false>(expr, ctx, skip, batch_size);
    } else if (ObDoubleType == arg_type) {
      ret = is_floor
      ? do_eval_batch_ceil_floor<3,true>(expr, ctx, skip, batch_size)
      : do_eval_batch_ceil_floor<3,false>(expr, ctx, skip, batch_size);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg type", K(ret), K(arg_type));
    }
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
