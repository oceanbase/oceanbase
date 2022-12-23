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
 * This file contains implementation for st_geometryfromtext.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_GEOMETRYFROMTEXT_
#define OCEANBASE_SQL_OB_EXPR_ST_GEOMETRYFROMTEXT_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_expr_st_geomfromtext.h"


namespace oceanbase
{
namespace sql
{
class ObExprSTGeometryFromText : public ObExprSTGeomFromText
{
public:
  explicit ObExprSTGeometryFromText(common::ObIAllocator &alloc);
  virtual ~ObExprSTGeometryFromText();
  static int eval_st_geometryfromtext(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTGeometryFromText);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_GEOMETRYFROMTEXT_