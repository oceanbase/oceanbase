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
 * This file contains implementation for _st_clipbybox2d.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_CLIPBYBOX2D_
#define OCEANBASE_SQL_OB_EXPR_ST_CLIPBYBOX2D_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "observer/omt/ob_tenant_srs.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTClipByBox2D : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTClipByBox2D(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTClipByBox2D();
  virtual int calc_result_type2(
      ObExprResType &type, ObExprResType &type1, ObExprResType &type2, common::ObExprTypeCtx &type_ctx) const override;
  static int eval_priv_st_clipbybox2d(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  static int process_input_geometry(omt::ObSrsCacheGuard &srs_guard, const ObExpr &expr, ObEvalCtx &ctx, ObIAllocator &allocator, bool &is_null_res, ObGeometry *&geo1,
      ObGeometry *&geo2, const ObSrsItem *&srs1, const ObSrsItem *&srs2);
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTClipByBox2D);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_ST_CLIPBYBOX2D_