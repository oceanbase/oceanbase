/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_XOR_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_XOR_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
// bitwise xor
class ObExprBitXor : public ObBitwiseExprOperator
{
public:
  explicit  ObExprBitXor(common::ObIAllocator &alloc);
  virtual ~ObExprBitXor() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBitXor);
};
} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_XOR_
