/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 * This file contains implementation for st_pointfromtext.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_pointfromtext.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprSTPointFromText::ObExprSTPointFromText(ObIAllocator &alloc)
    : ObExprSTGeomFromText(alloc, T_FUN_SYS_ST_POINTFROMTEXT, N_ST_POINTFROMTEXT, MORE_THAN_ZERO, NOT_ROW_DIMENSION)
{
}

ObExprSTPointFromText::~ObExprSTPointFromText()
{
}

int ObExprSTPointFromText::eval_st_pointfromtext(const ObExpr &expr,
                                                 ObEvalCtx &ctx,
                                                 ObDatum &res)
{
  return eval_st_geomfromtext_common(expr, ctx, res, N_ST_POINTFROMTEXT);
}

int ObExprSTPointFromText::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                   const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_pointfromtext;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase