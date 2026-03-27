/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_STMT_ID_H_
#define OBDEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_STMT_ID_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprStmtId : public ObFuncExprOperator
{
public:
  explicit ObExprStmtId(common::ObIAllocator &alloc);
  virtual ~ObExprStmtId() {}

  virtual int calc_result_type0(ObExprResType &type,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_stmt_id(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprStmtId);

};
}//end namespace sql
}//end namespace oceanbase
#endif /* OBDEV_SRC_SQL_ENGINE_EXPR_OB_EXPR_STMT_ID_H_ */
