/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CHAR_LENGTH_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CHAR_LENGTH_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprCharLength : public ObFuncExprOperator
{
public:
  explicit  ObExprCharLength(common::ObIAllocator &alloc);
  virtual ~ObExprCharLength();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_char_length(const ObExpr &expr, ObEvalCtx &ctx, 
                              ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCharLength);
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CHAR_LENGTH_
