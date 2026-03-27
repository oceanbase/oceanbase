/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_OVERLAPS_H_
#define OCEANBASE_SQL_OB_EXPR_ST_OVERLAPS_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprSTOverlaps : public ObFuncExprOperator
{
public:
  explicit ObExprSTOverlaps(common::ObIAllocator &alloc);
  virtual ~ObExprSTOverlaps();
  virtual int calc_result_type2(ObExprResType &type, ObExprResType &type1, ObExprResType &type2,
      common::ObExprTypeCtx &type_ctx) const override;
  static int eval_st_overlaps(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  static int process_input_geometry(omt::ObSrsCacheGuard &srs_guard, const ObExpr &expr, ObEvalCtx &ctx, MultimodeAlloctor &allocator, ObGeometry *&geo1,
      ObGeometry *&geo2, bool &is_null_res, const ObSrsItem *&srs);
//   static int is_geometry_valid(
    //   ObGeometry *geo, ObIAllocator &allocator, const common::ObSrsItem *srs_item, bool &is_valid);
  DISALLOW_COPY_AND_ASSIGN(ObExprSTOverlaps);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_ST_OVERLAPS_H_