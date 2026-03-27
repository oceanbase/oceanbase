/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_geographyfromtext.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTGeographyFromText::ObExprPrivSTGeographyFromText(ObIAllocator &alloc)
    : ObExprPrivSTGeogFromText(alloc, T_FUN_SYS_PRIV_ST_GEOGRAPHYFROMTEXT, N_PRIV_ST_GEOGRAPHYFROMTEXT, 1, NOT_ROW_DIMENSION)
{
}

ObExprPrivSTGeographyFromText::~ObExprPrivSTGeographyFromText()
{
}

int ObExprPrivSTGeographyFromText::eval_priv_st_geographyfromtext(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_priv_st_geogfromtext_common(expr, ctx, res, N_PRIV_ST_GEOGRAPHYFROMTEXT);
}

int ObExprPrivSTGeographyFromText::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                           const ObRawExpr &raw_expr,
                                           ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_geographyfromtext;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase