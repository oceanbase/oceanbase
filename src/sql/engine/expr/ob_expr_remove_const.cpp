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

#include "ob_expr_remove_const.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprRemoveConst::ObExprRemoveConst(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_REMOVE_CONST, N_REMOVE_CONST, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

int ObExprRemoveConst::calc_result_type1(ObExprResType &type,
                                           ObExprResType &arg,
                                           common::ObExprTypeCtx &) const
{
  int ret = OB_SUCCESS;
  type = arg;
  return ret;
}

int ObExprRemoveConst::cg_expr(ObExprCGCtx &,
                                 const ObRawExpr &,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprRemoveConst::eval_remove_const;
  return ret;
}

int ObExprRemoveConst::eval_remove_const(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("expr evaluate parameter failed", K(ret));
  } else {
    expr_datum.set_datum(*arg);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
