/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_BIT_RIGHT_SHIFT_
#define OCEANBASE_SQL_ENGINE_EXPR_BIT_RIGHT_SHIFT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprBitRightShift : public ObBitwiseExprOperator
{
public:
  explicit  ObExprBitRightShift(common::ObIAllocator &alloc);
  virtual ~ObExprBitRightShift() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBitRightShift);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_BIT_RIGHT_SHIFT_ */

