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
#include "sql/engine/expr/ob_expr_fun_values.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/ob_sql_utils.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprFunValues::ObExprFunValues(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_VALUES, N_VALUES, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprFunValues::~ObExprFunValues()
{
}

int ObExprFunValues::calc_result_type1(ObExprResType &type,
                                       ObExprResType &text,
                                       common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_type(text.get_type());
  type.set_collation_level(text.get_collation_level());
  type.set_collation_type(text.get_collation_type());
  type.set_accuracy(text.get_accuracy());
  return OB_SUCCESS;
}

int ObExprFunValues::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprFunValues::eval_values;
  return ret;
}

int ObExprFunValues::eval_values(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  // values() meaningful only in insert on duplicate update clause (extra_ set to 1),
  // return NULL otherwise.
  int ret = OB_SUCCESS;
  if (expr.extra_) {
    // in insert update scope
    ObDatum *arg = NULL;
    if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
      LOG_WARN("evaluate parameter value failed", K(ret));
    } else {
      expr_datum.set_datum(*arg);
    }
  } else {
    expr_datum.set_null();
  }

  return ret;
}

}
}
