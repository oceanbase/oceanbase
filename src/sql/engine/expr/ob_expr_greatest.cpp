/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_greatest.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprGreatest::ObExprGreatest(ObIAllocator &alloc)
    : ObExprLeastGreatest(alloc,
                           T_FUN_SYS_GREATEST,
                           N_GREATEST,
                           MORE_THAN_ZERO)
{
}

//same type params
int ObExprGreatest::calc_greatest(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
    ret = ObExprLeastGreatest::calc_oracle(expr, ctx, expr_datum, false);
  } else {
    ret = ObExprLeastGreatest::calc_mysql(expr, ctx, expr_datum, false);
  }
  return ret;
}

}
}
