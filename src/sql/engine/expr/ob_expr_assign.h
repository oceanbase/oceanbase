/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OB_EXPR_ASSIGN_H_
#define _OCEANBASE_SQL_OB_EXPR_ASSIGN_H_
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/ob_name_def.h"
namespace oceanbase
{
namespace sql
{
class ObExprAssign : public ObFuncExprOperator
{
public:
  explicit  ObExprAssign(common::ObIAllocator &alloc);
  virtual ~ObExprAssign();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &key,
                                ObExprResType &value,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const override;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprAssign);
};

} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_ASSIGN_H_
