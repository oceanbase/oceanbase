/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SYS_OP_OPNSIZE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SYS_OP_OPNSIZE_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSysOpOpnsize : public ObFuncExprOperator
{
public:
  explicit  ObExprSysOpOpnsize(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_OP_OPNSIZE, "sys_op_opnsize", 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
  {}
  virtual ~ObExprSysOpOpnsize() {}

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_sys_op_opnsize_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                      ObDatum &res);
  static int calc_sys_op_opnsize(const ObExpr *expr, const ObDatum *arg, int64_t &size);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSysOpOpnsize);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SYS_OP_OPNSIZE_
