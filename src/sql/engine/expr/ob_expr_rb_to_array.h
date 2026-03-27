/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for rb_to_array.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_RB_TO_ARRAY_
#define OCEANBASE_SQL_OB_EXPR_RB_TO_ARRAY_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"


namespace oceanbase
{
namespace sql
{
class ObExprRbToArray : public ObFuncExprOperator
{
public:
  explicit ObExprRbToArray(common::ObIAllocator &alloc);
  virtual ~ObExprRbToArray();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_rb_to_array(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_rb_to_array_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound);
  static int rb_to_array(const ObExpr &expr,
                         ObEvalCtx &ctx,
                         ObIAllocator &alloc,
                         ObRoaringBitmap *&rb,
                         ObIArrayType *&arr_res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbToArray);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_RB_TO_ARRAY_