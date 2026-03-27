/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_PART_ID_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_PART_ID_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{

namespace sql
{

class ObExprPartId : public ObFuncExprOperator
{
public:
  explicit ObExprPartId(common::ObIAllocator &alloc);
  virtual ~ObExprPartId() {}

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_part_id(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPartId);

};
}//end namespace sql
}//end namespace oceanbase
#endif
