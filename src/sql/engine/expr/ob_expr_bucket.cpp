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

#include "sql/engine/expr/ob_expr_bucket.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

ObExprBucket::ObExprBucket(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc,
                       T_FUN_SYS_BUCKET,
                       N_BUCKET,
                       2,
                       VALID_FOR_GENERATED_COL,
                       NOT_ROW_DIMENSION) {}

int ObExprBucket::calc_result_type2(ObExprResType &type,
                                      ObExprResType &type1,
                                      ObExprResType &type2,
                                      common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type1.set_type(type.get_type());
  type2.set_type(type.get_type());
  return ret;
}

int ObExprBucket::calc_bucket_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  return OB_SUCCESS;
}

int ObExprBucket::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_bucket_expr;
  return ret;
}

} // namespace sql
} // namespace oceanbase
