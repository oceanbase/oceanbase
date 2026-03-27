/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for _st_geographyfromtext.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_PRIV_ST_GEOGRAPHYFROMTEXT_
#define OCEANBASE_SQL_OB_EXPR_PRIV_ST_GEOGRAPHYFROMTEXT_
#include "sql/engine/expr/ob_expr_priv_st_geogfromtext.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTGeographyFromText : public ObExprPrivSTGeogFromText
{
public:
  explicit ObExprPrivSTGeographyFromText(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTGeographyFromText();
  static int eval_priv_st_geographyfromtext(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTGeographyFromText);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_PRIV_ST_GEOGRAPHYFROMTEXT_