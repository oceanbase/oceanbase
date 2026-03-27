/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains declare of the treat.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_TREAT_H_
#define OCEANBASE_SQL_OB_EXPR_TREAT_H_

#include "common/object/ob_obj_type.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprTreat: public ObFuncExprOperator
{
public:
  explicit  ObExprTreat(common::ObIAllocator &alloc);
  virtual ~ObExprTreat();

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_treat(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  DECLARE_SET_LOCAL_SESSION_VARS;

private:
    DISALLOW_COPY_AND_ASSIGN(ObExprTreat);
};

} //sql
} //oceanbase
#endif  //OCEANBASE_SQL_OB_EXPR_TREAT_H_
