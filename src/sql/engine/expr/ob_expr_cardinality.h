/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_EXPR_CARDINALITY
#define OB_EXPR_CARDINALITY

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprCardinality : public ObFuncExprOperator
{
public:
  explicit ObExprCardinality(common::ObIAllocator& alloc);
  virtual ~ObExprCardinality();
  int assign(const ObExprOperator &other);
  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_card(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCardinality);
};
}
}

#endif
