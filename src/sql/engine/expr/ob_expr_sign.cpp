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
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSign::ObExprSign(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_OP_SIGN, N_SIGN, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSign::~ObExprSign()
{
}

int ObExprSign::calc_result_type1(ObExprResType &type,
                                         ObExprResType &text,
                                         common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  //keep enumset as origin type
  if (lib::is_oracle_mode()) {
    type.set_number();
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_scale());
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY2[ORACLE_MODE][ObNumberType].get_precision());
  } else {
    type.set_int();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }
  const ObSQLSessionInfo *session = type_ctx.get_session();
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

int ObExprSign::calc(ObObj &result, double val)
{
  if (0 == val) {
    result.set_int(0);
  } else {
    result.set_int(val < 0 ? -1 : 1);
  }
  return OB_SUCCESS;
}

int calc_sign_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg_datum = NULL;
  const ObObjType &arg_type = expr.args_[0]->datum_meta_.type_;
  const ObCollationType &arg_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  const ObObjType &res_type = expr.datum_meta_.type_;
  const ObObjTypeClass &arg_tc = ob_obj_type_class(arg_type);
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
        if (is_mysql_mode() && 0 == v) {
          res_int = 0;
        } else {
          res_int = v < 0 ? -1 : 1;
        }
        break;
      }
      case ObDoubleTC: {
        double v = arg_datum->get_double();
        if (is_mysql_mode() && 0 == v) {
          res_int = 0;
        } else {
          res_int = v < 0 ? -1 : 1;
        }
        break;
      }
      case ObBitTC: {
        res_int = arg_datum->get_bit() == 0 ? 0 : 1;
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

int ObExprSign::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_sign_expr;
  return ret;
}

} //namespace sql
} //namespace oceanbase
