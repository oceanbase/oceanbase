/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_SQL_OB_EXPR_ST_ASMVTGEOM_
#define OCEANBASE_SQL_OB_EXPR_ST_ASMVTGEOM_
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_func_box.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

namespace oceanbase
{
namespace common
{
class ObGeometry;
}
namespace sql
{
class ObExprPrivSTAsMVTGeom : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTAsMVTGeom(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTAsMVTGeom();
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types, int64_t param_num,
      common::ObExprTypeCtx &type_ctx) const override;
  static int eval_priv_st_asmvtgeom(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

private:
  static int clip_geometry(ObGeometry *geo, lib::MemoryContext &mem_ctx, ObGeoType basic_type,
      int32_t extent, int32_t buffer, bool clip_geom, bool &is_null_res, ObGeometry *&res_geo);
  static int get_basic_type(ObGeometry *geo, ObGeoType &basic_type);
  static int snap_geometry_to_grid(ObGeometry *&geo, ObIAllocator &allocator, bool use_floor);
  static int split_geo_to_basic_type(
      ObGeometry &in_geo, ObIAllocator &allocator, ObGeoType basic_type, ObGeometry *&split_geo);
  static int affine_to_tile_space(ObGeometry *&geo, const ObGeogBox *bounds, int32_t extent);
  static int process_input_geometry(const ObExpr &expr, ObEvalCtx &ctx, MultimodeAlloctor &allocator,
    bool &is_null_res, ObGeometry *&geo1, ObGeometry *&geo2, int32_t &extent, int32_t &buffer, bool &clip_geom);
  static int get_bounds(lib::MemoryContext &mem_ctx, ObGeometry &geo, ObGeogBox *&bounds);
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTAsMVTGeom);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_ST_ASMVTGEOM_