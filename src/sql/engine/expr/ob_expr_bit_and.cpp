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
#include "ob_expr_bit_and.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;
namespace oceanbase
{
namespace sql
{

ObExprBitAnd::ObExprBitAnd(ObIAllocator &alloc)
    : ObBitwiseExprOperator(alloc, T_OP_BIT_AND, N_BIT_AND, 2, NOT_ROW_DIMENSION)
{};

ObExprBitAnd::ObExprBitAnd(ObIAllocator &alloc,
                           ObExprOperatorType type,
                           const char *name,
                           int32_t param_num,
                           int32_t dimension)
    : ObBitwiseExprOperator(alloc, type, name, param_num, dimension)
{};

ObExprBitAndOra::ObExprBitAndOra(ObIAllocator &alloc)
    : ObExprBitAnd(alloc, T_OP_BIT_AND, N_BIT_AND_ORACLE, 2, NOT_ROW_DIMENSION)
{};

int ObExprBitAnd::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  const BitOperator op = BIT_AND;
  if (OB_FAIL(cg_bitwise_expr(expr_cg_ctx, raw_expr, rt_expr, op))) {
    LOG_WARN("cg_bitwise_expr failed", K(ret), K(rt_expr), K(op));
  }

  return ret;
}

}/* ns sql*/
}/* ns oceanbase */


