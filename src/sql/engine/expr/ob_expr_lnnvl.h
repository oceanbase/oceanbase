/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_LNNVL_H
#define _OB_EXPR_LNNVL_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprFuncLnnvl : public ObFuncExprOperator
{
public:
  explicit  ObExprFuncLnnvl(common::ObIAllocator &alloc);
  virtual ~ObExprFuncLnnvl();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_lnnvl(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFuncLnnvl);
};

inline int ObExprFuncLnnvl::calc_result_type1(ObExprResType &type,
                                              ObExprResType &type1,
                                              common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type1);
  type.set_tinyint();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].precision_);
  type1.set_calc_type(common::ObTinyIntType);
  return common::OB_SUCCESS;
}


}
}
#endif /*OB_SQL_EXPR_FUNC_END*/
