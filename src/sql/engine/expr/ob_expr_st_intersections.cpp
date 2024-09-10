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
 * This file contains implementation for eval_st_intersections.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "ob_expr_st_intersections.h"
#include "lib/geo/ob_geo_elevation_visitor.h"
#include "lib/geo/ob_geo_func_utils.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprSTIntersections::ObExprSTIntersections(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_INTERSECTIONS, N_ST_INTERSECTIONS, 2, 
          VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprSTIntersections::~ObExprSTIntersections()
{}

int ObExprSTIntersections::calc_result_type2(ObExprResType &type,
                                             ObExprResType &type1,
                                             ObExprResType &type2,
                                             common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (type1.get_type() == ObNullType) {
  } else if (!ob_is_geometry(type1.get_type()) && !ob_is_string_type(type1.get_type())) {
    type1.set_calc_type(ObVarcharType);
    type1.set_calc_collation_type(CS_TYPE_BINARY);
  }
  if (type2.get_type() == ObNullType) {
  } else if (!ob_is_geometry(type2.get_type()) && !ob_is_string_type(type2.get_type())) {
    type2.set_calc_type(ObVarcharType);
    type2.set_calc_collation_type(CS_TYPE_BINARY);
  }
  type.set_geometry();
  type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  return ret;
}

int ObExprSTIntersections::eval_st_intersections(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  bool is_geo1_empty = false;
  bool is_geo2_empty = false;
  ObGeometry *geo1_3d = nullptr;
  ObGeometry *geo2_3d = nullptr;
  bool is_null_res = false;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = nullptr;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObGeometry *diff_res = nullptr;
  bool is_empty_res = false;

  if (OB_FAIL(ObGeoExprUtils::process_input_geometry(srs_guard, expr, ctx, temp_allocator, 
                                geo1_3d, geo2_3d, is_null_res, srs, N_ST_INTERSECTIONS))) {
    LOG_WARN("fail to process input geometry", K(ret));
  } else if (!is_null_res) {
    ObGeometry *geo1 = nullptr;
    ObGeometry *geo2 = nullptr;
    bool is_3d_geo1 = ObGeoTypeUtil::is_3d_geo_type(geo1_3d->type());
    bool is_3d_geo2 = ObGeoTypeUtil::is_3d_geo_type(geo2_3d->type());

    if ((is_3d_geo1 && !is_3d_geo2) || (!is_3d_geo1 && is_3d_geo2)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("mixed dimension geometries", K(ret), K(is_3d_geo1), K(is_3d_geo2));
    } else if (is_3d_geo1 && is_3d_geo2) {
      if (OB_FAIL(ObGeoTypeUtil::convert_geometry_3D_to_2D(
              srs, temp_allocator, geo1_3d, ObGeoBuildFlag::GEO_DEFAULT, geo1))) {
        LOG_WARN("fail to convert 3D geometry to 2D", K(ret));
      } else if (OB_FAIL(ObGeoTypeUtil::convert_geometry_3D_to_2D(
              srs, temp_allocator, geo2_3d, ObGeoBuildFlag::GEO_DEFAULT, geo2))) {
        LOG_WARN("fail to convert 3D geometry to 2D", K(ret));
      }
    } else {
      geo1 = geo1_3d;
      geo2 = geo2_3d;
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo1, is_geo1_empty))
               || OB_FAIL(ObGeoExprUtils::check_empty(geo2, is_geo2_empty))) {
      LOG_WARN("check geo empty failed", K(ret));
    } else if (is_geo1_empty || is_geo2_empty){
      is_empty_res = true;
    } else {
      // eval by bg
      ObGeoEvalCtx gis_context(&temp_allocator, srs);
      if (OB_FAIL(gis_context.append_geo_arg(geo1)) || OB_FAIL(gis_context.append_geo_arg(geo2))) {
        LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
      } else if (OB_FAIL(
                     ObGeoFunc<ObGeoFuncType::Intersections>::geo_func::eval(gis_context, diff_res))) {
        LOG_WARN("eval st intersections failed", K(ret));
        ObGeoExprUtils::geo_func_error_handle(ret, N_ST_INTERSECTIONS);
      } else if (OB_FAIL(ObGeoExprUtils::check_empty(diff_res, is_empty_res))) {
        LOG_WARN("check geo empty failed", K(ret));
      }
    } // end else

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_empty_res) {
      // 2D return GEOMETRYCOLLECTION EMPTY, 3D return GEOMETRYCOLLECTION Z EMPTY
      if (OB_FAIL(ObGeoExprUtils::create_3D_empty_collection(temp_allocator, geo1->get_srid(), is_3d_geo1,
                    geo1->crs() == ObGeoCRS::Geographic, diff_res))) {
        LOG_WARN("fail to create 3D empty collection", K(ret));
      }
    } else {
      if (geo1->crs() == ObGeoCRS::Cartesian
         && OB_FAIL(ObGeoFuncUtils::remove_duplicate_multi_geo<ObCartesianGeometrycollection>(diff_res, temp_allocator, srs))) {
        // should not do simplify in symdifference functor, it may affect
        // ObGeoFuncUtils::ob_geo_gc_union
        LOG_WARN("fail to simplify result", K(ret));
      } else if (geo1->crs() == ObGeoCRS::Geographic
         && OB_FAIL(ObGeoFuncUtils::remove_duplicate_multi_geo<ObGeographGeometrycollection>(diff_res, temp_allocator, srs))) {
        // should not do simplify in symdifference functor, it may affect
        // ObGeoFuncUtils::ob_geo_gc_union
        LOG_WARN("fail to simplify result", K(ret));
      } else if (is_3d_geo1 && is_3d_geo2) {
        // populate Z coordinates
        ObGeoElevationVisitor visitor(temp_allocator, srs);
        ObGeometry *diff_res_bin = nullptr;
        if (OB_FAIL(visitor.init(*geo1_3d, *geo2_3d))) {
          LOG_WARN("fail to init elevation visitor", K(ret));
        } else if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(temp_allocator, diff_res, diff_res_bin, srs))) {
          LOG_WARN("fail to do tree to bin", K(ret));
        } else if (OB_FAIL(diff_res_bin->do_visit(visitor))) {
          LOG_WARN("fail to do elevation visitor", K(ret));
        } else if (OB_FAIL(visitor.get_geometry_3D(diff_res))) {
          LOG_WARN("failed get geometry 3D", K(ret));
        }
      }
    } // end else
  } // end else if(!is_null_res)

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_null_res) {
    res.set_null();
  } else {
    ObString res_wkb;
    if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*diff_res, expr, ctx, srs, res_wkb))) {
      LOG_WARN("failed to write geometry to wkb", K(ret));
    } else {
      res.set_string(res_wkb);
    }
  }

  return ret;
}

int ObExprSTIntersections::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_intersections;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase