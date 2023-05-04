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
#include "sql/engine/expr/ob_expr_connect_by_root.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "share/ob_i_sql_expression.h"

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
