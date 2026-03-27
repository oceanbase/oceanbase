/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for _st_numinteriorrings.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_NUMINTERIORRINGS_
#define OCEANBASE_SQL_OB_EXPR_ST_NUMINTERIORRINGS_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTNumInteriorRings : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTNumInteriorRings(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTNumInteriorRings();
  virtual int calc_result_type1(
      ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;
  static int eval_priv_st_numinteriorrings(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTNumInteriorRings);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_ST_NUMINTERIORRINGS_