/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for array_concat.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_CONCAT
#define OCEANBASE_SQL_OB_EXPR_ARRAY_CONCAT

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"


namespace oceanbase
{
namespace sql
{
class ObExprArrayConcat : public ObFuncExprOperator
{
public:
  explicit ObExprArrayConcat(common::ObIAllocator &alloc);
  explicit ObExprArrayConcat(common::ObIAllocator &alloc, ObExprOperatorType type,
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArrayConcat();

  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_array_concat(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_concat_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_concat_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:

  DISALLOW_COPY_AND_ASSIGN(ObExprArrayConcat);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_CONCAT