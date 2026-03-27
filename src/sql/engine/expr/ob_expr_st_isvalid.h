/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_ISVALID_
#define OCEANBASE_SQL_OB_EXPR_ST_ISVALID_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSTIsValid : public ObFuncExprOperator
{
public:
  explicit ObExprSTIsValid(common::ObIAllocator &alloc);
  virtual ~ObExprSTIsValid() {}
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_st_isvalid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTIsValid);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_ISVALID_
