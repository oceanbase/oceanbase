/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_OBVERSION_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_OBVERSION_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprObVersion : public ObStringExprOperator
{
public:
  explicit ObExprObVersion(common::ObIAllocator &alloc);
  virtual ~ObExprObVersion();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;

  // for new engine
  static int eval_version(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_OBVERSION_ */
