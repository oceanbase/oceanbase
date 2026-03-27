/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_BIT_LEFT_SHIFT_
#define OCEANBASE_SQL_ENGINE_EXPR_BIT_LEFT_SHIFT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprBitLeftShift : public ObBitwiseExprOperator
{
public:
  explicit  ObExprBitLeftShift(common::ObIAllocator &alloc);
  virtual ~ObExprBitLeftShift() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBitLeftShift);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_BIT_LEFT_SHIFT_ */

