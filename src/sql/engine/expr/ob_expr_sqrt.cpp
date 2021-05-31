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
#include "ob_expr_sqrt.h"
#include <cmath>
#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {

ObExprSqrt::ObExprSqrt(ObIAllocator& alloc) : ObFuncExprOperator(alloc, T_FUN_SYS_SQRT, N_SQRT, 1, NOT_ROW_DIMENSION)
{}

ObExprSqrt::~ObExprSqrt()
{}

int ObExprSqrt::calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (NOT_ROW_DIMENSION != row_dimension_ || ObMaxType == type1.get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else if (!share::is_oracle_mode()) {
    type.set_double();
  } else {
    if (ob_is_real_type(type1.get_type())) {
      type.set_type(type1.get_type());
    } else {
      type.set_number();
    }
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][type.get_type()].get_scale());
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][type.get_type()].get_precision());
  }
  type1.set_calc_type(type.get_type());
  ObExprOperator::calc_result_flag1(type, type1);
  return ret;
}

int ObExprSqrt::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (obj.is_null()) {
    result.set_null();
  } else if (lib::is_oracle_mode()) {
    if (obj.is_number()) {
      const number::ObNumber arg_number = obj.get_number();
      number::ObNumber result_number;
      common::ObIAllocator& allocator = *(expr_ctx.calc_buf_);
      if (OB_FAIL(arg_number.sqrt(result_number, allocator))) {
        LOG_WARN("sqrt failed", K(ret), K(arg_number), K(result_number));
      } else {
        result.set_number(result_number);
        LOG_DEBUG("ObExprSqrt, ObNumber path end successfully");
      }
    } else if (obj.is_double()) {
      double value = obj.get_double();
      if (value < 0) {
        ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      } else if (value == -0) {
        result.set_double(-0);
      } else {
        result.set_double(sqrt(value));
      }
    } else if (obj.is_float()) {
      float value = obj.get_float();
      if (value < 0) {
        ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      } else if (value == -0) {
        result.set_float(-0);
      } else {
        result.set_float(sqrtf(value));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("obj type should be number or float, double", K(obj), K(ret));
    }
  } else {
    if (obj.is_double()) {
      double value = obj.get_double();
      if (value < 0) {
        result.set_null();
      } else {
        result.set_double(sqrt(value));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("obj type should be double", K(obj), K(ret));
    }
  }
  return ret;
}

int calc_sqrt_expr_mysql(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    double val = arg->get_double();
    if (val < 0) {
      res_datum.set_null();
    } else {
      res_datum.set_double(std::sqrt(val));
    }
  }
  return ret;
}

int calc_sqrt_expr_oracle_double(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    if (ObDoubleType == arg_type) {
      double val = arg->get_double();
      if (val < 0) {
        ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      } else {
        res_datum.set_double(std::sqrt(val));
      }
    } else if (ObFloatType == arg_type) {
      float val = arg->get_float();
      if (val < 0) {
        ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
      } else {
        res_datum.set_float(sqrtf(val));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg type", K(ret), K(arg_type));
    }
  }
  return ret;
}

int calc_sqrt_expr_oracle_number(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    const number::ObNumber arg_nmb(arg->get_number());
    number::ObNumber res_nmb;
    if (OB_FAIL(arg_nmb.sqrt(res_nmb, ctx.get_reset_tmp_alloc()))) {
      LOG_WARN("calc sqrt failed", K(ret), K(arg_nmb), K(res_nmb));
    } else {
      res_datum.set_number(res_nmb);
    }
  }
  return ret;
}

int ObExprSqrt::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_ of expr", K(ret), K(rt_expr));
  } else {
    ObObjType arg_res_type = rt_expr.args_[0]->datum_meta_.type_;
    if (share::is_oracle_mode()) {
      if (ObDoubleType == arg_res_type || ObFloatType == arg_res_type) {
        rt_expr.eval_func_ = calc_sqrt_expr_oracle_double;
      } else if (ObNumberType == arg_res_type) {
        rt_expr.eval_func_ = calc_sqrt_expr_oracle_number;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arg type must be double or number in oracle mode", K(ret), K(arg_res_type), K(rt_expr));
      }
    } else {
      if (ObDoubleType == arg_res_type) {
        rt_expr.eval_func_ = calc_sqrt_expr_mysql;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arg_res_type must be double in mysql mode", K(ret), K(arg_res_type));
      }
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
