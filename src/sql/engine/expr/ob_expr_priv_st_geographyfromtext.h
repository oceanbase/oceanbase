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