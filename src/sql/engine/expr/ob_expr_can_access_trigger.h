/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_CAN_ACCESS_TRIGGER_H
#define _OB_EXPR_CAN_ACCESS_TRIGGER_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprCanAccessTrigger : public ObExprOperator
{
public:
  explicit  ObExprCanAccessTrigger(common::ObIAllocator &alloc);
  virtual ~ObExprCanAccessTrigger();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int can_access_trigger(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCanAccessTrigger);
};
}
}
#endif