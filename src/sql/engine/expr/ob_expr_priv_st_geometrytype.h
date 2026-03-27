/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for _st_geometrytype.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_GEOMETRYTYPE_
#define OCEANBASE_SQL_OB_EXPR_ST_GEOMETRYTYPE_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTGeometryType : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTGeometryType(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTGeometryType();
  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const;
  static int eval_priv_st_geometrytype(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
private:
  static const int32_t MAX_TYPE_LEN = 22; // ST_GeometryCollection
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTGeometryType);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_ST_GEOMETRYTYPE_