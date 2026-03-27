/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_bit_left_shift.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprBitLeftShift::ObExprBitLeftShift(ObIAllocator &alloc) :
    ObBitwiseExprOperator(alloc, T_OP_BIT_LEFT_SHIFT, N_BIT_LEFT_SHIFT, 2, NOT_ROW_DIMENSION)
  {
  }


int ObExprBitLeftShift::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  const BitOperator op = BIT_LEFT_SHIFT;
  if (OB_FAIL(cg_bitwise_expr(expr_cg_ctx, raw_expr, rt_expr, op))) {
    LOG_WARN("cg_bitwise_expr failed", K(ret), K(rt_expr), K(op));
  }

  return ret;
}
}/* ns sql*/
}/* ns oceanbase */


