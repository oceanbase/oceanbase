/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for _st_asewkb expr.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_PRIV_ST_ASEWKB_
#define OCEANBASE_SQL_OB_EXPR_PRIV_ST_ASEWKB_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprStPrivAsEwkb : public ObFuncExprOperator
{
public:
  explicit ObExprStPrivAsEwkb(common::ObIAllocator &alloc);
  virtual ~ObExprStPrivAsEwkb();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int eval_priv_st_as_ewkb(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprStPrivAsEwkb);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_PRIV_ST_ASEWKB_