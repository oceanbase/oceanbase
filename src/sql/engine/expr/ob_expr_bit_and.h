/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_BIT_AND_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_BIT_AND_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprBitAnd : public ObBitwiseExprOperator
{
public:
  explicit ObExprBitAnd(common::ObIAllocator &alloc);
  ObExprBitAnd(common::ObIAllocator &alloc,
               ObExprOperatorType type,
               const char *name,
               int32_t param_num,
               int32_t dimension);
  virtual ~ObExprBitAnd() {};
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBitAnd);
};

class ObExprBitAndOra : public ObExprBitAnd
{
public:
  explicit  ObExprBitAndOra(common::ObIAllocator &alloc);
  // use cg_expr in ObExprBitAnd
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBitAndOra);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_BIT_AND_ */

