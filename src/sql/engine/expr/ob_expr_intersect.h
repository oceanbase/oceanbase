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
 * This file contains implementation for array_intersect.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_INTERSECT
#define OCEANBASE_SQL_OB_EXPR_ARRAY_INTERSECT

#include "sql/engine/expr/ob_expr_array_union.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprArrayIntersect : public ObExprArraySetOperation
{
public:
  explicit ObExprArrayIntersect(common::ObIAllocator &alloc);
  virtual ~ObExprArrayIntersect();
  static int eval_array_intersect(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_intersect_batch(const ObExpr &expr, 
                ObEvalCtx &ctx,
                const ObBitVector &skip, 
                const int64_t batch_size);
  static int eval_array_intersect_vector(const ObExpr &expr, 
                ObEvalCtx &ctx,
                const ObBitVector &skip, 
                const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                  const ObRawExpr &raw_expr,
                  ObExpr &rt_expr) const override;
private: 
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayIntersect);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_INTERSECT