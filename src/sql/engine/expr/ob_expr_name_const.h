/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OB_EXPR_NAME_CONST_H_
#define _OCEANBASE_SQL_OB_EXPR_NAME_CONST_H_
#include "lib/ob_name_def.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{
class ObExprNameConst : public ObFuncExprOperator
{
public:
  explicit  ObExprNameConst(common::ObIAllocator &alloc);
  virtual ~ObExprNameConst();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &expr) const override;
  static int eval_name_const(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprNameConst);
};

} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_NAME_CONST_H_
