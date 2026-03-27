/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_BOOL_
#define OCEANBASE_SQL_ENGINE_EXPR_BOOL_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprBool : public ObLogicalExprOperator
{
public:
  explicit ObExprBool(common::ObIAllocator &alloc);
  virtual ~ObExprBool();

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
  static int calc_vector_bool_expr(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   const ObBitVector &skip,
                                   const EvalBound &bound);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBool);
};

}
}

#endif // OCEANBASE_SQL_ENGINE_EXPR_BOOL_
