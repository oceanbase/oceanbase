/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_connect_by_root.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
ObExprConnectByRoot::ObExprConnectByRoot(common::ObIAllocator &alloc)
  : ObExprOperator(alloc, T_OP_CONNECT_BY_ROOT, N_CONNECT_BY_ROOT, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE) {};
int ObExprConnectByRoot::calc_result_type1(ObExprResType &type, ObExprResType &type1, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type = type1;
  UNUSED(type_ctx);
  return ret;
}


int ObExprConnectByRoot::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = ObExprConnectByRoot::eval_connect_by_root;
  return ret;
}

int ObExprConnectByRoot::eval_connect_by_root(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, arg))) {
    LOG_WARN("expression evaluate parameters failed", K(ret));
  } else {
    expr_datum.set_datum(*arg);
  }
  return ret;
}
