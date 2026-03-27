/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SYS_PRIVILEGE_CHECK_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SYS_PRIVILEGE_CHECK_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprSysPrivilegeCheck : public ObFuncExprOperator
{
public:
  explicit  ObExprSysPrivilegeCheck(common::ObIAllocator &alloc);
  virtual ~ObExprSysPrivilegeCheck();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_sys_privilege_check(
      const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  static int check_show_priv(bool &allow_show,
                             ObExecContext &exec_ctx,
                             const common::ObString &level,
                             const uint64_t tenant_id,
                             const common::ObString &db_name,
                             const common::ObString &obj_name,
                             const int64_t routine_type = 0);
  DISALLOW_COPY_AND_ASSIGN(ObExprSysPrivilegeCheck);
};
}
}
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SYS_PRIVILEGE_CHECK_
