/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ELEMENT_AT
#define OCEANBASE_SQL_OB_EXPR_ELEMENT_AT

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprElementAt : public ObFuncExprOperator
{
public:
  explicit ObExprElementAt(common::ObIAllocator &alloc);
  virtual ~ObExprElementAt();

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_element_at(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_element_at_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                   const ObBitVector &skip, const int64_t batch_size);
  static int eval_element_at_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                    const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprElementAt);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ELEMENT_AT