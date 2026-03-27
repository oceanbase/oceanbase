/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_geometryfromtext.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprSTGeometryFromText::ObExprSTGeometryFromText(ObIAllocator &alloc)
    : ObExprSTGeomFromText(alloc, T_FUN_SYS_ST_GEOMETRYFROMTEXT, N_ST_GEOMETRYFROMTEXT, MORE_THAN_ZERO, NOT_ROW_DIMENSION)
{
}

ObExprSTGeometryFromText::~ObExprSTGeometryFromText()
{
}

int ObExprSTGeometryFromText::eval_st_geometryfromtext(const ObExpr &expr,
                                                       ObEvalCtx &ctx,
                                                       ObDatum &res)
{
  return eval_st_geomfromtext_common(expr, ctx, res, N_ST_GEOMETRYFROMTEXT);
}

int ObExprSTGeometryFromText::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_geometryfromtext;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase