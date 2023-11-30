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
 * This file contains implementation for _st_asmvtgeom.
 */
#ifndef OCEANBASE_SQL_OB_EXPR_ST_ASMVTGEOM_
#define OCEANBASE_SQL_OB_EXPR_ST_ASMVTGEOM_
#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/geo/ob_geo_func_box.h"

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
  static int clip_geometry(ObGeometry *geo, ObIAllocator &allocator, ObGeoType basic_type,
      int32_t extent, int32_t buffer, bool clip_geom, bool &is_null_res, ObGeometry *&res_geo);
  static int get_basic_type(ObGeometry *geo, ObGeoType &basic_type);
  static int snap_geometry_to_grid(ObGeometry *&geo, ObIAllocator &allocator, bool use_floor);
  static int split_geo_to_basic_type(
      ObGeometry *&geo, ObIAllocator &allocator, ObGeoType basic_type, ObGeometry *&split_geo);
  static int affine_to_tile_space(ObGeometry *&geo, const ObGeogBox *bounds, int32_t extent);
  static int process_input_geometry(const ObExpr &expr, ObEvalCtx &ctx, bool &is_null_res,
      ObGeometry *&geo1, ObGeogBox *&bounds, int32_t &extent, int32_t &buffer, bool &clip_geom);
  DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTAsMVTGeom);
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_EXPR_ST_ASMVTGEOM_