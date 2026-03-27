/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for array_compact.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_COMPACT
#define OCEANBASE_SQL_OB_EXPR_ARRAY_COMPACT

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprArrayCompact : public ObFuncExprOperator
{
public:
  explicit ObExprArrayCompact(common::ObIAllocator &alloc);
  explicit ObExprArrayCompact(common::ObIAllocator &alloc, ObExprOperatorType type,
                              const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArrayCompact();
  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_array_compact(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_compact_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                      const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_compact_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                       const ObBitVector &skip, const EvalBound &bound);
  static int eval_compact(ObIAllocator &tmp_allocator, ObEvalCtx &ctx,
                          ObIArrayType *src_arr, ObIArrayType *res_arr);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayCompact);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_COMPACT