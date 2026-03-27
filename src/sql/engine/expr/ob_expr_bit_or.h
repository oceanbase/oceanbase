/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_BITWISE_OR_H_
#define _OB_EXPR_BITWISE_OR_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprBitOr : public ObBitwiseExprOperator
{
public:
  explicit  ObExprBitOr(common::ObIAllocator &alloc);
  virtual ~ObExprBitOr() {};
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBitOr);
};
}
}
#endif  /* _OB_EXPR_BITWISE_OR_H_ */
