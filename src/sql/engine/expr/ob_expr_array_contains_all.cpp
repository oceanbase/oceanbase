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
 * This file contains implementation for array_contains_all.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_contains_all.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_type.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{

ObExprArrayContainsAll::ObExprArrayContainsAll(ObIAllocator &alloc)
    : ObExprArrayOverlaps(alloc, T_FUNC_SYS_ARRAY_CONTAINS_ALL, N_ARRAY_CONTAINS_ALL, 2, ObArithExprOperator::NOT_ROW_DIMENSION)
{
}

ObExprArrayContainsAll::~ObExprArrayContainsAll()
{
}

int ObExprArrayContainsAll::eval_array_contains_all(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_array_relations(expr, ctx, CONTAINS_ALL, res);
}

int ObExprArrayContainsAll::eval_array_contains_all_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                                          const ObBitVector &skip, const int64_t batch_size)
{
  return eval_array_relations_batch(expr, ctx, skip, batch_size, CONTAINS_ALL);
}

int ObExprArrayContainsAll::eval_array_contains_all_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                           const ObBitVector &skip, const EvalBound &bound)
{
  return eval_array_relation_vector(expr, ctx, skip, bound, CONTAINS_ALL);
}

int ObExprArrayContainsAll::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_contains_all;
  rt_expr.eval_batch_func_ = eval_array_contains_all_batch;
  rt_expr.eval_vector_func_ = eval_array_contains_all_vector;

  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
