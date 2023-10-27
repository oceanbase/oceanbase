/**
 * Copyright (c) 2023 OceanBase
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

#include "sql/engine/expr/ob_expr_name_const.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprNameConst::ObExprNameConst(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_NAME_CONST, N_NAME_CONST, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         false, false)
{
}

ObExprNameConst::~ObExprNameConst()
{
}

int ObExprNameConst::calc_result_type2(ObExprResType &type,
                                        ObExprResType &type1,
                                        ObExprResType &type2,
                                        ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObRawExpr *raw_expr = type_ctx.get_raw_expr();
  if (OB_ISNULL(raw_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(raw_expr));
  } else {
    const ObRawExpr *name_expr = raw_expr->get_param_expr(0);
    const ObRawExpr *value_expr = raw_expr->get_param_expr(1);
    if (OB_ISNULL(name_expr) || OB_ISNULL(value_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(name_expr), K(value_expr));
    } else if (!name_expr->is_basic_const_expr_mysql() ||
              T_QUESTIONMARK == name_expr->get_expr_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_NAME_CONST);
      LOG_WARN("name is not simple const expr", K(ret), K(*name_expr));
    } else if (!value_expr->is_basic_const_expr_mysql()) {
      if (T_OP_NEG != value_expr->get_expr_type() && T_FUN_SYS_SET_COLLATION != value_expr->get_expr_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_NAME_CONST);
        LOG_WARN("value is not simple const expr or collate function", K(ret), K(*value_expr));
      } else {
        value_expr = value_expr->get_param_expr(0);
        if (OB_ISNULL(value_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(value_expr));
        } else if ((T_FUN_SYS_CAST == value_expr->get_expr_type()) && CM_IS_IMPLICIT_CAST(value_expr->get_extra())) {
          //for cases like -'1'
          if (OB_ISNULL(value_expr->get_param_expr(0))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret), K(value_expr->get_param_expr(0)));
          } else {
            value_expr = value_expr->get_param_expr(0);
          }
        }
      }
    }
    //check value type
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!value_expr->is_basic_const_expr_mysql()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_NAME_CONST);
      LOG_WARN("value is not simple const expr", K(ret), K(*value_expr));
    } else {
      const ObExprResType &orig_value_type = value_expr->get_result_type();
      if (orig_value_type.get_type() >= ObDateTimeType && orig_value_type.get_type() <= ObTimeType) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_NAME_CONST);
        LOG_WARN("value can't have prefix like TIME, DATE and TIMESTAMP", K(ret), K(orig_value_type));
      } else {
        // do nothing
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ob_is_integer_type(type2.get_type())) {
      type.set_int();
      type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
      type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
    } else {
      type.assign(type2);
    }
  }
  return ret;
}

int ObExprNameConst::eval_name_const(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *name_param = NULL;
  ObDatum *value_param = NULL;
  ObExpr *name_expr = expr.args_[0];
  if (OB_FAIL(expr.eval_param_value(ctx, name_param, value_param))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (OB_ISNULL(name_param) || OB_ISNULL(value_param) || OB_ISNULL(name_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("name or value is NULL", K(ret), K(name_param), K(value_param));
  } else if (name_param->is_null()) {
    ObString fun_name(N_NAME_CONST);
    ret = OB_ERR_RESERVED_SYNTAX;
    LOG_USER_ERROR(OB_ERR_RESERVED_SYNTAX, fun_name.length(), fun_name.ptr());
    LOG_WARN("name should not be NULL", K(ret), K(*name_param));
  } else {
    //check if name < 0 to be compatiable with mysql
    common::ObObjType name_type = name_expr->datum_meta_.get_type();
    bool is_neg = false;
    if (ObFloatType == name_type) {
      is_neg = name_param->get_float() < 0;
    } else if (ObDoubleType == name_type) {
      is_neg = name_param->get_double() < 0;
    } else if (name_type >= ObTinyIntType && name_type <= ObIntType) {
      is_neg = name_param->get_int() < 0;
    } else if (ObNumberType == name_type) {
      number::ObNumber name_nmb(name_param->get_number());
      is_neg = name_nmb.is_negative();
    }
    if (is_neg) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_NAME_CONST);
      LOG_WARN("name is negtive",K(ret),K(name_param));
    } else {
      res = *value_param;
    }
  }
  return ret;
}

int ObExprNameConst::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  expr.eval_func_ = eval_name_const;
  return ret;
}

} //namespace sql
} //namespace oceanbase
