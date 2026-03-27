/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_SPATIAL_CELLID_
#define OCEANBASE_SQL_OB_EXPR_SPATIAL_CELLID_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSpatialCellid : public ObFuncExprOperator
{
public:
  explicit ObExprSpatialCellid(common::ObIAllocator &alloc);
  virtual ~ObExprSpatialCellid();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_result1(common::ObObj &result,
                           const common::ObObj &obj,
                           common::ObExprCtx &expr_ctx) const;
  static int eval_spatial_cellid(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSpatialCellid);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_SPATIAL_CELLID_