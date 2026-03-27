/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for array_max expression.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_EXTREME
#define OCEANBASE_SQL_OB_EXPR_ARRAY_EXTREME

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{

class ObExprArrayExtreme : public ObFuncExprOperator
{
public:
  explicit ObExprArrayExtreme(common::ObIAllocator &alloc, ObExprOperatorType type, const char *name);
  virtual ~ObExprArrayExtreme();

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_array_extreme(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, bool is_max = true);
  static int eval_array_extreme_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                    const ObBitVector &skip, const int64_t batch_size,
                                    bool is_max = true);
  static int eval_array_extreme_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                     const ObBitVector &skip, const EvalBound &bound,
                                     bool is_max = true);
  static int calc_extreme(ObIArrayType* src_arr, ObObj &res_obj, bool is_max = true);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayExtreme);
};

class ObExprArrayMax : public ObExprArrayExtreme
{
public:
  explicit ObExprArrayMax(common::ObIAllocator &alloc);
  virtual ~ObExprArrayMax();

  static int eval_array_max(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_max_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                  const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_max_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                   const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayMax);
};

class ObExprArrayMin : public ObExprArrayExtreme
{
public:
  explicit ObExprArrayMin(common::ObIAllocator &alloc);
  virtual ~ObExprArrayMin();

  static int eval_array_min(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_min_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                  const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_min_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                   const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayMin);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_EXTREME