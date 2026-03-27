/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_bit_or.h"

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;

ObExprBitOr::ObExprBitOr(ObIAllocator &alloc):
  ObBitwiseExprOperator(alloc, T_OP_BIT_OR, N_BIT_OR, 2, NOT_ROW_DIMENSION) {};


int ObExprBitOr::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  const BitOperator op = BIT_OR;
  if (OB_FAIL(cg_bitwise_expr(expr_cg_ctx, raw_expr, rt_expr, op))) {
    LOG_WARN("cg_bitwise_expr failed", K(ret), K(rt_expr), K(op));
  }

  return ret;
}

}
}
