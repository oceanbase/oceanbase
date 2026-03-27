/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for _st_asgeojson.
 */
#ifndef OCEANBASE_SQL_OB_EXPR_ST_ASGEOJSON_
#define OCEANBASE_SQL_OB_EXPR_ST_ASGEOJSON_
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
class ObExprSTAsGeoJson : public ObFuncExprOperator
{
public:
  explicit ObExprSTAsGeoJson(common::ObIAllocator &alloc);
  virtual ~ObExprSTAsGeoJson();
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types, int64_t param_num,
      common::ObExprTypeCtx &type_ctx) const override;
  static int eval_st_asgeojson(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  static int process_input_params(const ObExpr &expr, ObEvalCtx &ctx, MultimodeAlloctor &allocator,
      ObGeometry *&geo, bool &is_null_res, ObGeoSrid& srid, uint32_t &max_dec_digits,
      uint8_t &flag);
  DISALLOW_COPY_AND_ASSIGN(ObExprSTAsGeoJson);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_ST_ASGEOJSON_
