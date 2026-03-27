/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_PRIV_ST_SETSRID_
#define OCEANBASE_SQL_OB_EXPR_PRIV_ST_SETSRID_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_st_srid.h"


namespace oceanbase
{
namespace sql
{
class ObExprPrivSTSetSRID : public ObExprSTSRID
{
public:
  explicit ObExprPrivSTSetSRID(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTSetSRID();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_priv_st_setsrid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTSetSRID);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_PRIV_ST_SETSRID_