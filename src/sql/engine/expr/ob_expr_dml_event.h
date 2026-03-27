/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_DML_EVENT_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_DML_EVENT_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprDmlEvent : public ObFuncExprOperator {

public:
  ObExprDmlEvent();
  explicit ObExprDmlEvent(common::ObIAllocator& alloc);
  virtual ~ObExprDmlEvent();
  virtual int calc_result_typeN(ObExprResType &type, 
                                ObExprResType *dml,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_dml_event(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  private:
    DISALLOW_COPY_AND_ASSIGN(ObExprDmlEvent);
};
}
}
#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_DML_EVENT_H_ */
