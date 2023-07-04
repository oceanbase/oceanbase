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

#include "sql/engine/expr/ob_expr_fun_default.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprFunDefault::ObExprFunDefault(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_DEFAULT, N_DEFAULT, 5, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  disable_operand_auto_cast();
}

ObExprFunDefault::~ObExprFunDefault()
{
}

int ObExprFunDefault::calc_result_typeN(ObExprResType &type,
                                        ObExprResType *types,
                                        int64_t param_num,
                                        common::ObExprTypeCtx &type_ctx) const
{
  //no need to set calc type of types

  int ret = OB_SUCCESS;
  //objs[0] type
  //objs[1] collation_type
  //objs[2] accuray_expr
  //objs[3] nullable_expr
  //objs[4] default_value
  //objs[5] column_info
  const ObRawExpr *raw_expr = type_ctx.get_raw_expr();
  if (OB_ISNULL(raw_expr) || OB_ISNULL(raw_expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN,"unexpected null", K(ret));
  } else if (raw_expr->get_param_expr(0)->is_column_ref_expr()) {
    type.set_type(types[0].get_type());
    type.set_collation_type(types[0].get_collation_type());
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_accuracy(types[0].get_accuracy());
  } else if (param_num != ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO
      && param_num != ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument, param_num should be 5 or 6", K(param_num));
  } else {
    type.set_type(types[0].get_type());
    type.set_collation_type(types[1].get_collation_type());
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_accuracy(types[2].get_accuracy());
    types[ObExprColumnConv::VALUE_EXPR].set_calc_type(type.get_type());
    types[ObExprColumnConv::VALUE_EXPR].set_calc_collation_type(type.get_collation_type());
    types[ObExprColumnConv::VALUE_EXPR].set_calc_collation_level(type.get_collation_level());
  }
  return ret;
}

int ObExprFunDefault::calc_default_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                        ObDatum &res)
{
  int ret = OB_SUCCESS;
  //objs[0] type
  //objs[1] collation_type
  //objs[2] accuray_expr
  //objs[3] nullable_expr
  //objs[4] default_value
  //objs[5] column_info
  ObDatum *nullable = NULL;
  ObDatum *def = NULL;
  if (OB_UNLIKELY(expr.arg_cnt_ != ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO
      && expr.arg_cnt_ != ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    SQL_ENG_LOG(WARN, "arg cnt must be 5", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.args_[3]->eval(ctx, nullable)) ||
             OB_FAIL(expr.args_[4]->eval(ctx, def))) {
    SQL_ENG_LOG(WARN, "eval arg failed", K(ret));
  } else if (nullable->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "nullable param should be bool type", K(ret));
  } else if (def->is_null()) {
    if (!lib::is_oracle_mode() && !nullable->get_bool()) {
      ret = OB_ERR_NO_DEFAULT_FOR_FIELD;
      SQL_ENG_LOG(WARN, "Field doesn't have a default value", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    res.set_datum(*def);
  }
  return ret;
}

int ObExprFunDefault::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_default_expr;
  return ret;
}

}
}
