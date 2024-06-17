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
 * This file contains implementation for eval_st_difference.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_ibin.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "ob_expr_st_difference.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "lib/geo/ob_geo_elevation_visitor.h"
#include "lib/geo/ob_geo_func_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSTDifference::ObExprSTDifference(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_DIFFERENCE, N_ST_DIFFERENCE, 2,
          VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprSTDifference::~ObExprSTDifference()
{}

int ObExprSTDifference::calc_result_type2(ObExprResType &type, ObExprResType &type1,
    ObExprResType &type2, common::ObExprTypeCtx &type_ctx) const
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

int ObExprSTDifference::process_input_geometry(const ObExpr &expr, ObEvalCtx &ctx,
    ObIAllocator &allocator, ObGeometry *&geo1, ObGeometry *&geo2, bool &is_null_res,
    const ObSrsItem *&srs)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum1 = nullptr;
  ObDatum *gis_datum2 = nullptr;
  ObExpr *gis_arg1 = expr.args_[0];
  ObExpr *gis_arg2 = expr.args_[1];
  ObObjType input_type1 = gis_arg1->datum_meta_.type_;
  ObObjType input_type2 = gis_arg2->datum_meta_.type_;
  is_null_res = false;
  if (OB_FAIL(gis_arg1->eval(ctx, gis_datum1)) || OB_FAIL(gis_arg2->eval(ctx, gis_datum2))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (gis_datum1->is_null() || gis_datum2->is_null()) {
    is_null_res = true;
  } else {
    ObGeoType type1;
    ObGeoType type2;
    uint32_t srid1;
    uint32_t srid2;
    ObString wkb1 = gis_datum1->get_string();
    ObString wkb2 = gis_datum2->get_string();
    omt::ObSrsCacheGuard srs_guard;
    bool is_geo1_valid = false;
    bool is_geo2_valid = false;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
            *gis_datum1,
            gis_arg1->datum_meta_,
            gis_arg1->obj_meta_.has_lob_header(),
            wkb1))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb1));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
                   *gis_datum2,
                   gis_arg2->datum_meta_,
                   gis_arg2->obj_meta_.has_lob_header(),
                   wkb2))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb2));
    } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb1, type1, srid1))) {
      if (ret == OB_ERR_GIS_INVALID_DATA) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_DIFFERENCE);
      }
      LOG_WARN("get type and srid from wkb failed", K(wkb1), K(ret));
    } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb2, type2, srid2))) {
      if (ret == OB_ERR_GIS_INVALID_DATA) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_DIFFERENCE);
      }
      LOG_WARN("get type and srid from wkb failed", K(wkb2), K(ret));
    } else if (srid1 != srid2) {
      ret = OB_ERR_GIS_DIFFERENT_SRIDS;
      LOG_WARN("srid not the same", K(ret), K(srid1), K(srid2));
      LOG_USER_ERROR(OB_ERR_GIS_DIFFERENT_SRIDS, N_ST_DIFFERENCE, srid1, srid2);
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(
                   ctx, srs_guard, wkb1, srs, true, N_ST_DIFFERENCE))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb1));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(allocator,
                   wkb1,
                   geo1,
                   srs,
                   N_ST_DIFFERENCE,
                   ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT | GEO_RESERVE_3D))) {
      LOG_WARN("get first geo by wkb failed", K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(allocator,
                   wkb2,
                   geo2,
                   srs,
                   N_ST_DIFFERENCE,
                   ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT | GEO_RESERVE_3D))) {
      LOG_WARN("get second geo by wkb failed", K(ret));
    }
  }
  return ret;
}

int ObExprSTDifference::eval_st_difference(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_geo1_empty = false;
  bool is_geo2_empty = false;
  ObGeometry *geo1_3d = nullptr;
  ObGeometry *geo2_3d = nullptr;
  bool is_null_res = false;
  const ObSrsItem *srs = nullptr;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObGeometry *diff_res = nullptr;
  bool is_empty_res = false;
  if (OB_FAIL(
          process_input_geometry(expr, ctx, temp_allocator, geo1_3d, geo2_3d, is_null_res, srs))) {
    LOG_WARN("fail to process input geometry", K(ret));
  } else if (!is_null_res) {
    ObGeometry *geo1 = nullptr;
    ObGeometry *geo2 = nullptr;
    bool is_3d_geo1 = ObGeoTypeUtil::is_3d_geo_type(geo1_3d->type());
    bool is_3d_geo2 = ObGeoTypeUtil::is_3d_geo_type(geo2_3d->type());
    if ((is_3d_geo1 && !is_3d_geo2) || (!is_3d_geo1 && is_3d_geo2)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_WARN("mixed dimension geometries", K(ret), K(is_3d_geo1), K(is_3d_geo2));
    } else if (is_3d_geo1) {
      if (OB_FAIL(ObGeoTypeUtil::convert_geometry_3D_to_2D(
              srs, temp_allocator, geo1_3d, ObGeoBuildFlag::GEO_DEFAULT, geo1))) {
        LOG_WARN("fail to convert 3D geometry to 2D", K(ret));
      }
    } else {
      geo1 = geo1_3d;
    }
    if (OB_FAIL(ret)) {
    } else if (is_3d_geo2) {
      if (OB_FAIL(ObGeoTypeUtil::convert_geometry_3D_to_2D(
              srs, temp_allocator, geo2_3d, ObGeoBuildFlag::GEO_DEFAULT, geo2))) {
        LOG_WARN("fail to convert 3D geometry to 2D", K(ret));
      }
    } else {
      geo2 = geo2_3d;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo1, is_geo1_empty))
               || OB_FAIL(ObGeoExprUtils::check_empty(geo2, is_geo2_empty))) {
      LOG_WARN("check geo empty failed", K(ret));
    } else if (is_geo1_empty) {
      is_empty_res = true;
    } else if (is_geo2_empty) {
      ObGeoToTreeVisitor to_tree(&temp_allocator);
      if (OB_FAIL(geo1->do_visit(to_tree))) {
        LOG_WARN("fail to transfer geo1 to tree", K(ret));
      } else {
        diff_res = to_tree.get_geometry();
      }
    } else {
      ObGeoEvalCtx gis_context(&temp_allocator, srs);
      if (OB_FAIL(gis_context.append_geo_arg(geo1)) || OB_FAIL(gis_context.append_geo_arg(geo2))) {
        LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
      } else if (OB_FAIL(
                     ObGeoFunc<ObGeoFuncType::Difference>::geo_func::eval(gis_context, diff_res))) {
        LOG_WARN("eval st difference failed", K(ret));
        ObGeoExprUtils::geo_func_error_handle(ret, N_ST_DIFFERENCE);
      } else if (OB_FAIL(ObGeoExprUtils::check_empty(diff_res, is_empty_res))) {
        LOG_WARN("check geo empty failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
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
      }  else if (geo1->crs() == ObGeoCRS::Geographic
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
    }
  }

  if (OB_FAIL(ret)) {
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

int ObExprSTDifference::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_difference;
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
