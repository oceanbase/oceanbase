/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_FUN_LAST_EXEC_ID
#define OCEANBASE_SQL_OB_EXPR_FUN_LAST_EXEC_ID
 #include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprLastExecId :  public ObStringExprOperator
{
public:
  explicit  ObExprLastExecId(common::ObIAllocator &alloc);
  virtual ~ObExprLastExecId();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_last_exec_id(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLastExecId);
};
} //namespace sql
} //namespace oceanbase
#endif


