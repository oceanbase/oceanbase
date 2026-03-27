/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for array_remove.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_REMOVE
#define OCEANBASE_SQL_OB_EXPR_ARRAY_REMOVE

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"


namespace oceanbase
{
namespace sql
{
class ObExprArrayRemove : public ObFuncExprOperator
{
public:
  explicit ObExprArrayRemove(common::ObIAllocator &alloc);
  explicit ObExprArrayRemove(common::ObIAllocator &alloc, ObExprOperatorType type,
                                const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArrayRemove();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_array_remove_int64_t(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_remove_float(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_remove_double(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_remove_ObString(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_remove_array(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_remove_array_batch(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_remove_batch_int64_t(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_remove_batch_float(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_remove_batch_double(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_remove_batch_ObString(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_remove_vector_int64_t(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
  static int eval_array_remove_vector_float(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
  static int eval_array_remove_vector_double(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
  static int eval_array_remove_vector_ObString(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
  static int eval_array_remove_array_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:

  DISALLOW_COPY_AND_ASSIGN(ObExprArrayRemove);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_REMOVE