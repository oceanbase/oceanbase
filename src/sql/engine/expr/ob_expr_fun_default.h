/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_EXPR_FUN_DEFAULT_H_
#define _OB_SQL_EXPR_FUN_DEFAULT_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprFunDefault : public ObFuncExprOperator
{
public:
  explicit  ObExprFunDefault(common::ObIAllocator &alloc);
  virtual ~ObExprFunDefault();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  static int calc_default_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprFunDefault);
};

}
}
#endif /* _OB_SQL_EXPR_FUN_DEFAULT_H_ */
