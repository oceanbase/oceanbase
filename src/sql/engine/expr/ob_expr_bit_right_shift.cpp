/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_bit_right_shift.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObExprBitRightShift::ObExprBitRightShift(ObIAllocator &alloc) :
    ObBitwiseExprOperator(alloc, T_OP_BIT_RIGHT_SHIFT, N_BIT_RIGHT_SHIFT, 2, NOT_ROW_DIMENSION) {}


int ObExprBitRightShift::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  const BitOperator op = BIT_RIGHT_SHIFT;
  if (OB_FAIL(cg_bitwise_expr(expr_cg_ctx, raw_expr, rt_expr, op))) {
    LOG_WARN("cg_bitwise_expr failed", K(ret), K(rt_expr), K(op));
  }

  return ret;
}
}/* ns sql*/
}/* ns oceanbase */


