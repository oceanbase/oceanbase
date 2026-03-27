/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TABLET_AUTOINC_NEXTVAL
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TABLET_AUTOINC_NEXTVAL
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/ob_tablet_autoincrement_service.h"

namespace oceanbase
{
namespace sql
{
class ObExprTabletAutoincNextval : public ObFuncExprOperator
{
public:
  explicit  ObExprTabletAutoincNextval(common::ObIAllocator &alloc);
  virtual ~ObExprTabletAutoincNextval() override;

  virtual int calc_result_type0(ObExprResType &type,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_nextval(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprTabletAutoincNextval);

};
}//end namespace sql
}//end namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TABLET_AUTOINC_NEXTVAL
