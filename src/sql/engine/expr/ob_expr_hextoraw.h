/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HEXTORAW_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HEXTORAW_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
#include "lib/number/ob_number_v2.h"
namespace oceanbase
{
namespace sql
{
class ObExprHextoraw : public ObStringExprOperator
{
public:
  explicit  ObExprHextoraw(common::ObIAllocator &alloc);
  virtual ~ObExprHextoraw();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const;
  static int calc_hextoraw_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprHextoraw);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HEXTORAW_ */
