/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_PRIV_ST_POINT_
#define OCEANBASE_SQL_OB_EXPR_PRIV_ST_POINT_

#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTPoint : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTPoint(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTPoint();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_priv_st_point(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTPoint);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_PRIV_ST_POINT_