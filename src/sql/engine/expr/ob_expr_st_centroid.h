/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for st_centroid.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_CENTROID_
#define OCEANBASE_SQL_OB_EXPR_ST_CENTROID_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSTCentroid : public ObFuncExprOperator
{
public:
  explicit ObExprSTCentroid(common::ObIAllocator &alloc);
  virtual ~ObExprSTCentroid() {}
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_st_centroid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTCentroid);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_CENTROID_
