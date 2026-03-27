/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_CHAR_H_
#define OCEANBASE_SQL_ENGINE_EXPR_CHAR_H_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprChar : public ObFuncExprOperator
{
public:
  explicit  ObExprChar(common::ObIAllocator &alloc);
  virtual ~ObExprChar();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const override;
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  int calc_result_type(ObExprResType &type, ObExprResType &type1) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprChar);
};
}
}

#endif /* OCEANBASE_SQL_ENGINE_EXPR_CHAR_H_ */
