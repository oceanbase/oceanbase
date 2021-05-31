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
#include "ob_expr_sign.h"
#include "sql/engine/expr/ob_expr_util.h"
#include <cmath>
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {

static constexpr double DOUBLE_EPS = 1e-8;
static constexpr float FLOAT_EPS = 1e-8;

ObExprSign::ObExprSign(ObIAllocator& alloc) : ObFuncExprOperator(alloc, T_OP_SIGN, N_SIGN, 1, NOT_ROW_DIMENSION)
{}

ObExprSign::~ObExprSign()
{}

int ObExprSign::calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  // keep enumset as origin type
  if (lib::is_oracle_mode()) {
    type.set_number();
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_scale());
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_precision());
  } else {
    type.set_int();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    if (ob_is_numeric_type(text.get_type())) {
      text.set_calc_type(text.get_type());
    } else {
      if (is_oracle_mode()) {
        text.set_calc_type(ObNumberType);
      } else {
        text.set_calc_type(ObDoubleType);
      }
    }
    ObExprOperator::calc_result_flag1(type, text);
  }
  return ret;
}

int ObExprSign::calc(ObObj& result, double val)
{
  if (fabs(val) < FLOAT_EPS) {
    result.set_int(0);
  } else {
    result.set_int(val < 0 ? -1 : 1);
  }
  return OB_SUCCESS;
}

int ObExprSign::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else if (lib::is_oracle_mode()) {
    ObObjType input_type = input_types_.at(0).get_calc_meta().get_type();
    ObCollationType input_cs_type = input_types_.at(0).get_calc_meta().get_collation_type();
    if (obj.is_null()) {
      if (cast_supported(input_type, input_cs_type, ObNumberType, CS_TYPE_BINARY)) {
        result.set_null();
      } else {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("invalid type to Number", K(input_type));
      }
      /*
       * The following two branches are used to deal with the problem that
       * floating-point number's precision is too high to result overflow
       * while converting ObNumber, and go directly to floating-point
       * number's comparison
       * */
    } else if (ob_is_double_tc(obj.get_type())) {
      number::ObNumber res_nmb;
      int64_t res_int = (obj.get_double() < 0 ? -1 : 1);
      if (OB_FAIL(res_nmb.from(res_int, *expr_ctx.calc_buf_))) {
        LOG_WARN("fail to get number", K(ret));
      } else {
        result.set_number(res_nmb);
      }
    } else if (ob_is_float_tc(obj.get_type())) {
      number::ObNumber res_nmb;
      int64_t res_int = (obj.get_float() < 0 ? -1 : 1);
      if (OB_FAIL(res_nmb.from(res_int, *expr_ctx.calc_buf_))) {
        LOG_WARN("fail to get number", K(ret));
      } else {
        result.set_number(res_nmb);
      }
    } else {
      number::ObNumber nmb;
      number::ObNumber res_nmb;
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_NUMBER_V2(obj, nmb);
      if (OB_SUCC(ret)) {
        int64_t res_int = nmb.is_negative() ? -1 : (nmb.is_zero() ? 0 : 1);
        if (OB_FAIL(res_nmb.from(res_int, *expr_ctx.calc_buf_))) {
          LOG_WARN("fail to get number", K(ret));
        } else {
          result.set_number(res_nmb);
        }
      }
    }
  } else {
    ObObjType type = obj.get_type();
    if (obj.is_null()) {
      result.set_null();
    } else if (ob_is_int_tc(type)) {
      int64_t value = obj.get_int();
      result.set_int(value < 0 ? -1 : (0 == value ? 0 : 1));
    } else if (ob_is_uint_tc(type)) {
      uint64_t value = obj.get_uint64();
      result.set_int(0 == value ? 0 : 1);
    } else if (ob_is_number_tc(type)) {
      number::ObNumber nmb = obj.get_number();
      result.set_int(nmb.is_negative() ? -1 : (nmb.is_zero() ? 0 : 1));
    } else {
      double value = 0.0;
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_DOUBLE_V2(obj, value);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to cast to double", K(obj), K(ret));
      } else if (OB_FAIL(calc(result, value))) {
        LOG_WARN("failed to calc for sign", K(obj), K(ret), K(value));
      }
    }
  }
  return ret;
}

int calc_sign_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* arg_datum = NULL;
  const ObObjType& arg_type = expr.args_[0]->datum_meta_.type_;
  const ObCollationType& arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  const ObObjType& res_type = expr.datum_meta_.type_;
  const ObObjTypeClass& arg_tc = ob_obj_type_class(arg_type);
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg_datum->is_null()) {
    if (is_oracle_mode()) {
      if (cast_supported(arg_type, arg_cs_type, ObNumberType, CS_TYPE_BINARY)) {
        res_datum.set_null();
      } else {
        ret = OB_INVALID_NUMERIC;
        LOG_WARN("invalid type to Number", K(arg_type));
      }
    } else {
      res_datum.set_null();
    }
  } else {
    int64_t res_int = 0;
    switch (arg_tc) {
      case ObIntTC: {
        int64_t v = arg_datum->get_int();
        res_int = v < 0 ? -1 : (0 == v ? 0 : 1);
        break;
      }
      case ObUIntTC: {
        res_int = arg_datum->get_uint64() == 0 ? 0 : 1;
        break;
      }
      case ObNumberTC: {
        number::ObNumber nmb(arg_datum->get_number());
        res_int = nmb.is_negative() ? -1 : (nmb.is_zero() ? 0 : 1);
        break;
      }
      case ObFloatTC: {
        float v = arg_datum->get_float();
        if (is_mysql_mode() && fabsf(v) < FLOAT_EPS) {
          res_int = 0;
        } else {
          res_int = v < 0 ? -1 : 1;
        }
        break;
      }
      case ObDoubleTC: {
        double v = arg_datum->get_double();
        if (is_mysql_mode() && fabs(v) < DOUBLE_EPS) {
          res_int = 0;
        } else {
          res_int = v < 0 ? -1 : 1;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected arg_type", K(ret), K(arg_type));
        break;
      }
    }
    if (ObNumberType == res_type) {
      number::ObNumber res_nmb;
      ObNumStackOnceAlloc tmp_alloc;
      if (OB_FAIL(res_nmb.from(res_int, tmp_alloc))) {
        LOG_WARN("get number from int failed", K(ret), K(res_int));
      } else {
        res_datum.set_number(res_nmb);
      }
    } else if (ObIntType == res_type) {
      res_datum.set_int(res_int);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected res_type", K(ret), K(res_type));
    }
  }
  return ret;
}

int ObExprSign::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_sign_expr;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
