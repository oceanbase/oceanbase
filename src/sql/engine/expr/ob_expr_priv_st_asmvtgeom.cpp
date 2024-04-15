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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_asmvtgeom.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/omt/ob_tenant_srs.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_utils.h"
#include "share/object/ob_obj_cast_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/geo/ob_geo_func_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTAsMVTGeom::ObExprPrivSTAsMVTGeom(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_ASMVTGEOM, N_PRIV_ST_ASMVTGEOM, MORE_THAN_ONE,
          NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprPrivSTAsMVTGeom::~ObExprPrivSTAsMVTGeom()
{}

int ObExprPrivSTAsMVTGeom::calc_result_typeN(ObExprResType &type, ObExprResType *types_stack,
    int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjType obj_type1 = types_stack[0].get_type();  // geometry
  ObObjType obj_type2 = types_stack[1].get_type();  // geometry
  if (ObHexStringType != obj_type1 && !ob_is_geometry(obj_type1) && !ob_is_null(obj_type1)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_ASMVTGEOM);
    LOG_WARN("invalid type", K(ret), K(obj_type1));
  } else if (ObHexStringType != obj_type2 && !ob_is_geometry(obj_type2)) {
    if (ob_is_null(obj_type2)) {
      ret = OB_ERR_NULL_INPUT;
      LOG_WARN("_ST_AsMVTGeom: Geometric bounds cannot be null", K(ret), K(obj_type2));
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("invalid input type extent", K(ret), K(obj_type2));
    }
  }
  // integer extent
  if (OB_SUCC(ret) && param_num >= 3) {
    ObObjType extent_type = types_stack[2].get_type();
    if (extent_type == ObTinyIntType
        || (!ob_is_integer_type(extent_type) && extent_type != ObVarcharType && !ob_is_null(extent_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("invalid input type extent", K(ret), K(extent_type));
    } else if (ob_is_string_type(extent_type)) {
      types_stack[2].set_calc_type(ObIntType);
    }
  }
  // integer buffer
  if (OB_SUCC(ret) && param_num >= 4) {
    ObObjType extent_type = types_stack[3].get_type();
    if (extent_type == ObTinyIntType
        || (!ob_is_integer_type(extent_type) && extent_type != ObVarcharType && !ob_is_null(extent_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("invalid input type extent", K(ret), K(extent_type));
    } else if (ob_is_string_type(extent_type)) {
      types_stack[3].set_calc_type(ObIntType);
    }
  }
  // boolean clip_geom
  if (OB_SUCC(ret) && param_num >= 5) {
    ObObjType extent_type = types_stack[4].get_type();
    if (extent_type != ObTinyIntType && extent_type != ObVarcharType
        && !ob_is_null(extent_type)) {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("invalid input type extent", K(ret), K(extent_type));
    } else if (ob_is_string_type(extent_type)) {
      types_stack[4].set_calc_type(ObTinyIntType);
    }
  }

  if (OB_SUCC(ret)) {
    ObCastMode cast_mode = type_ctx.get_cast_mode();
    cast_mode &= ~CM_WARN_ON_FAIL;         // make cast return error when fail
    cast_mode |= CM_STRING_INTEGER_TRUNC;  // make cast check range when string to int
    type_ctx.set_cast_mode(cast_mode);     // cast mode only do work in new sql engine cast frame.
    type.set_geometry();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  }
  return ret;
}

int ObExprPrivSTAsMVTGeom::process_input_geometry(const ObExpr &expr, ObEvalCtx &ctx,
    bool &is_null_res, ObGeometry *&geo1, ObGeogBox *&bounds, int32_t &extent, int32_t &buffer,
    bool &clip_geom)
{
  int ret = OB_SUCCESS;
  ObDatum *datum1 = nullptr;
  ObDatum *datum2 = nullptr;
  ObExpr *arg1 = expr.args_[0];
  ObExpr *arg2 = expr.args_[1];
  ObObjType type1 = arg1->datum_meta_.type_;
  ObObjType type2 = arg2->datum_meta_.type_;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs1 = nullptr;
  const ObSrsItem *srs2 = nullptr;
  ObEvalCtx::TempAllocGuard ctx_alloc_g(ctx);
  common::ObArenaAllocator &ctx_allocator = ctx_alloc_g.get_allocator();
  // ObArenaAllocator tmp_allocator;
  ObGeometry *geo2 = nullptr;

  // process two geometry
  if (ob_is_null(type1)) {
    is_null_res = true;
  } else if (ob_is_null(type2)) {
    ret = OB_ERR_NULL_INPUT;
    LOG_WARN("_ST_AsMVTGeom: Geometric bounds cannot be null", K(ret));
  } else if (OB_FAIL(arg1->eval(ctx, datum1)) || OB_FAIL(arg2->eval(ctx, datum2))) {
    LOG_WARN("fail to eval args", K(ret));
  } else if (datum1->is_null()) {
    is_null_res = true;
  } else if (datum2->is_null()) {
    ret = OB_ERR_NULL_INPUT;
    LOG_WARN("_ST_AsMVTGeom: Geometric bounds cannot be null", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObArenaAllocator &ctx_allocator = tmp_alloc_g.get_allocator();
    ObString wkb1 = datum1->get_string();
    ObString wkb2 = datum2->get_string();
    ObGeoEvalCtx box_ctx(&ctx_allocator);
    box_ctx.set_is_called_in_pg_expr(true);

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
            ctx_allocator, *datum1, arg1->datum_meta_, arg1->obj_meta_.has_lob_header(), wkb1))) {
      LOG_WARN(
          "fail to read real string data", K(ret), K(arg1->obj_meta_.has_lob_header()), K(wkb1));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(ctx_allocator,
                   *datum2,
                   arg2->datum_meta_,
                   arg2->obj_meta_.has_lob_header(),
                   wkb2))) {
      LOG_WARN(
          "fail to read real string data", K(ret), K(arg2->obj_meta_.has_lob_header()), K(wkb2));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb1, srs1, true, N_PRIV_ST_ASMVTGEOM))
              || OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb2, srs2, true, N_PRIV_ST_ASMVTGEOM))) {
      if (ret == OB_ERR_SRS_NOT_FOUND) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_ASMVTGEOM);
      }
      LOG_WARN("fail to get srs item", K(ret), K(wkb1), K(wkb2));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(ctx_allocator,
                   wkb1,
                   geo1,
                   srs1,
                   N_PRIV_ST_ASMVTGEOM,
                   ObGeoBuildFlag::GEO_NORMALIZE | ObGeoBuildFlag::GEO_CHECK_RING))) {
      LOG_WARN("get first geo by wkb failed", K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(ctx_allocator,
                   wkb2,
                   geo2,
                   srs2,
                   N_PRIV_ST_ASMVTGEOM,
                   ObGeoBuildFlag::GEO_DEFAULT | ObGeoBuildFlag::GEO_CHECK_RING))) {
      LOG_WARN("get second geo by wkb failed", K(ret));
    } else if (OB_NOT_NULL(srs1) && srs1->is_geographical_srs()) {
      ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
      LOG_USER_ERROR(OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS, N_PRIV_ST_ASMVTGEOM,
                  ObGeoTypeUtil::get_geo_name_by_type(geo1->type()));
      LOG_WARN("Geometry in geographical srs can not be input", K(ret), K(srs1));
    } else if (OB_NOT_NULL(srs2) && srs2->is_geographical_srs()) {
      ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
      LOG_USER_ERROR(OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS, N_PRIV_ST_ASMVTGEOM,
                  ObGeoTypeUtil::get_geo_name_by_type(geo2->type()));
      LOG_WARN("Geometry in geographical srs can not be input", K(ret), K(srs2));
    } else if (OB_FAIL(box_ctx.append_geo_arg(geo2))) {
      LOG_WARN("build gis context failed", K(ret), K(box_ctx.get_geo_count()));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(box_ctx, bounds))) {
      LOG_WARN("failed to do box functor failed", K(ret));
    } else if ((bounds->xmax - bounds->xmin) <= 0 || ((bounds->ymax - bounds->ymin) <= 0)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_ASMVTGEOM);
      LOG_WARN("_ST_AsMVTGeom: Geometric bounds are too small",
          K(ret),
          K(bounds->xmin),
          K(bounds->ymin),
          K(bounds->ymax),
          K(bounds->xmax));
    }
  }
  // process extent
  uint32_t num_args = expr.arg_cnt_;
  extent = 4096;  // default
  if (OB_SUCC(ret) && num_args >= 3) {
    ObDatum *datum = nullptr;
    if (OB_FAIL(expr.args_[2]->eval(ctx, datum))) {
      LOG_WARN("fail to eval second argument", K(ret));
    } else if (datum->is_null()) {
      // use default value
    } else if (datum->get_int() <= 0 || datum->get_int() > INT_MAX32) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "extent", N_PRIV_ST_ASMVTGEOM);
      LOG_WARN("_ST_AsMVTGeom: Extent must be greater than 0", K(ret), K(datum->get_int()));
    } else {
      extent = datum->get_int32();
    }
  }
  // process buffer
  buffer = 256;  // default
  if (OB_SUCC(ret) && num_args >= 4) {
    ObDatum *datum = nullptr;
    if (OB_FAIL(expr.args_[3]->eval(ctx, datum))) {
      LOG_WARN("fail to eval second argument", K(ret));
    } else if (datum->is_null()) {
      // use default value
    } else if (datum->get_int() < 0 || datum->get_int() > INT_MAX32) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "buffer", N_PRIV_ST_ASMVTGEOM);
      LOG_WARN("value is out of range", K(ret), K(datum->get_int()));
    } else {
      buffer = datum->get_int32();
    }
  }
  // process clip_geom
  clip_geom = true;  // default
  int8_t clip_num = 0;
  if (OB_SUCC(ret) && num_args >= 5) {
    ObDatum *datum = nullptr;
    if (OB_FAIL(expr.args_[4]->eval(ctx, datum))) {
      LOG_WARN("fail to eval second argument", K(ret));
    } else if (datum->is_null()) {
      // use default value
    } else if (FALSE_IT(clip_num = datum->get_tinyint())) {
    } else {
      clip_geom = datum->get_tinyint();
    }
  }
  return ret;
}

int ObExprPrivSTAsMVTGeom::get_basic_type(ObGeometry *geo, ObGeoType &basic_type)
{
  int ret = OB_SUCCESS;
  switch (geo->type()) {
    case ObGeoType::POINT:
    case ObGeoType::MULTIPOINT: {
      basic_type = ObGeoType::POINT;
      break;
    }
    case ObGeoType::LINESTRING:
    case ObGeoType::MULTILINESTRING: {
      basic_type = ObGeoType::LINESTRING;
      break;
    }
    case ObGeoType::POLYGON:
    case ObGeoType::MULTIPOLYGON: {
      basic_type = ObGeoType::POLYGON;
      break;
    }
    case ObGeoType::GEOMETRYCOLLECTION: {
      int8_t dimension = 0;
      ObIWkbGeomCollection *coll = reinterpret_cast<ObIWkbGeomCollection *>(geo);
      if (OB_FAIL(ObGeoTypeUtil::get_coll_dimension(coll, dimension))) {
        LOG_WARN("fail to get collection dimension", K(ret));
      } else {
        basic_type = static_cast<ObGeoType>(dimension + 1);
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid geo type", K(ret), K(geo->type()));
      break;
    }
  }
  return ret;
}

int ObExprPrivSTAsMVTGeom::affine_to_tile_space(
    ObGeometry *&geo, const ObGeogBox *bounds, int32_t extent)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(geo) || OB_ISNULL(bounds)) {
    ret = OB_ERR_NULL_INPUT;
    LOG_WARN("geometry and bounds cannot be null", K(ret), K(geo), K(bounds));
  } else {
    double x_fac = extent / (bounds->xmax - bounds->xmin);
    double y_fac = -(extent / (bounds->ymax - bounds->ymin));
    ObAffineMatrix affine = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    affine.x_fac1 = x_fac;
    affine.y_fac2 = y_fac;
    affine.z_fac3 = 1;
    affine.x_off = -bounds->xmin * x_fac;
    affine.y_off = -bounds->ymax * y_fac;
    if (OB_FAIL(ObGeoMVTUtil::affine_transformation(geo, affine))) {
      LOG_WARN("fail to do affine transformation",
          K(ret),
          K(x_fac),
          K(y_fac),
          K(affine.x_off),
          K(affine.y_off));
    }
  }
  return ret;
}

int ObExprPrivSTAsMVTGeom::split_geo_to_basic_type(
    ObGeometry *&geo, ObIAllocator &allocator, ObGeoType basic_type, ObGeometry *&split_geo)
{
  int ret = OB_SUCCESS;
  if (geo->type() == ObGeoType::GEOMETRYCOLLECTION) {
    ObCartesianMultipoint *mpt = NULL;
    ObCartesianMultilinestring *mls = NULL;
    ObCartesianMultipolygon *mpy = NULL;
    if (OB_FAIL(ObGeoFuncUtils::ob_geo_gc_split(
            allocator, *static_cast<ObCartesianGeometrycollection *>(geo), mpt, mls, mpy))) {
      LOG_WARN("failed to do gc split", K(ret));
    } else if (OB_ISNULL(mpt) || OB_ISNULL(mls) || OB_ISNULL(mpt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null geometry collection split", K(ret));
    } else {
      if (basic_type == ObGeoType::POLYGON) {
        split_geo = mpy;
      } else if (basic_type == ObGeoType::LINESTRING) {
        split_geo = mls;
      } else {
        split_geo = mpt;
      }
    }
  } else {
    split_geo = geo;
  }
  if (OB_SUCC(ret)
      && OB_FAIL((ObGeoFuncUtils::simplify_multi_geo<ObCartesianGeometrycollection>(
                 split_geo, allocator)))) {
    LOG_WARN("fail to simplify multi geometry", K(ret));
  }
  return ret;
}

int ObExprPrivSTAsMVTGeom::snap_geometry_to_grid(
    ObGeometry *&geo, ObIAllocator &allocator, bool use_floor)
{
  int ret = OB_SUCCESS;
  ObGeoGrid grid = {0, 0, 0, 1, 1, 0};
  if (OB_FAIL(ObGeoMVTUtil::snap_to_grid(geo, grid, use_floor))) {
    LOG_WARN("fail to do snap to grid", K(ret));
  } else if (OB_FAIL((ObGeoFuncUtils::simplify_multi_geo<ObCartesianGeometrycollection>(
                     geo, allocator)))) {
    LOG_WARN("fail to simplify multi geometry", K(ret));
  }
  return ret;
}

int ObExprPrivSTAsMVTGeom::clip_geometry(ObGeometry *geo, ObIAllocator &allocator,
    ObGeoType basic_type, int32_t extent, int32_t buffer, bool clip_geom, bool &is_null_res,
    ObGeometry *&res_geo)
{
  int ret = OB_SUCCESS;
  ObGeometry *basic_geo = nullptr;
  ObGeogBox *clip_box = nullptr;
  bool is_geo_empty = false;
  if (OB_FAIL(split_geo_to_basic_type(geo, allocator, ObGeoType::POLYGON, basic_geo))) {
    LOG_WARN("fail to split geo to basic type", K(ret));
  } else if (OB_FAIL(ObGeoExprUtils::check_empty(basic_geo, is_geo_empty))) {
    LOG_WARN("fail to check empty", K(ret));
  } else if (is_geo_empty) {
    is_null_res = true;
  } else if (basic_geo->type() != ObGeoType::POLYGON && basic_geo->type() != ObGeoType::MULTIPOLYGON
             && !clip_geom) {
    res_geo = basic_geo;
  } else {
    if (clip_geom) {
      if (OB_ISNULL(clip_box = OB_NEWx(ObGeogBox, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else {
        clip_box->xmax = clip_box->ymax = extent + static_cast<double>(buffer);
        clip_box->xmin = clip_box->ymin = -static_cast<double>(buffer);
        if (OB_FAIL(ObGeoBoxUtil::clip_by_box(*basic_geo, allocator, *clip_box, res_geo, true))) {
          LOG_WARN("fail to do clip by box", K(ret));
        } else if (OB_ISNULL(res_geo)) {
          is_null_res = true;
        } else if (OB_FAIL(ObGeoExprUtils::check_empty(res_geo, is_geo_empty))) {
          LOG_WARN("fail to check empty", K(ret));
        } else if (is_geo_empty) {
          is_null_res = true;
        }
      }
    } else {
      res_geo = basic_geo;
    }
    if (OB_FAIL(ret) || is_null_res) {
    } else if (basic_geo->type() == ObGeoType::POLYGON
               || basic_geo->type() == ObGeoType::MULTIPOLYGON) {
      ObGeometry *valid_poly = nullptr;
      if (OB_FAIL(ObGeoExprUtils::make_valid_polygon(res_geo, allocator, valid_poly))) {
        LOG_WARN("fail to make polygon valid", K(ret));
      } else {
        res_geo = valid_poly;
        if (OB_FAIL(snap_geometry_to_grid(res_geo, allocator, true))) {
          LOG_WARN("fail to snap geometry to grid", K(ret));
        }
      }
    } else if (OB_FAIL(snap_geometry_to_grid(res_geo, allocator, false))) {
      LOG_WARN("fail to snap geometry to grid", K(ret));
    }
  }
  return ret;
}

int ObExprPrivSTAsMVTGeom::eval_priv_st_asmvtgeom(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_null_res = false;
  ObGeometry *geo1 = nullptr;
  bool is_geo_empty = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObGeogBox *bounds = nullptr;
  int32_t extent = 4096;
  int32_t buffer = 256;
  bool clip_geom = true;
  ObGeometry *res_geo = nullptr;
  ObString res_wkb;

  if (OB_FAIL(process_input_geometry(
          expr, ctx, is_null_res, geo1, bounds, extent, buffer, clip_geom))) {
    LOG_WARN("fail to process input geometry", K(ret), K(geo1), K(is_null_res));
  } else if (is_null_res) {
    // do nothing
  } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo1, is_geo_empty))) {
    LOG_WARN("check geo empty failed", K(ret));
  } else if (is_geo_empty) {
    is_null_res = true;
  } else if (geo1->type() == ObGeoType::LINESTRING || geo1->type() == ObGeoType::MULTILINESTRING) {
    // pre-check
    ObGeogBox fast_box;
    bool has_fast_box = false;
    if (OB_FAIL(ObGeoBoxUtil::fast_box(geo1, fast_box, has_fast_box))) {
      LOG_WARN("fail to calculate fast box", K(ret), K(geo1->type()));
    } else if (has_fast_box) {
      double fast_box_width = fast_box.xmax - fast_box.xmin;
      double fast_box_height = fast_box.ymax - fast_box.ymin;
      double bounds_width = (bounds->xmax - bounds->xmin) / extent / 2.0;
      double bounds_height = (bounds->ymax - bounds->ymin) / extent / 2.0;
      if (fast_box_width < bounds_width && fast_box_height < bounds_height) {
        is_null_res = true;
      }
    }
  }

  ObGeoType basic_type = ObGeoType::GEOTYPEMAX;  // POINT/LINE/POLYGON
  ObGeometry *split_geo = nullptr;
  ObGeometry *geo_tree = nullptr;
  ObGeoToTreeVisitor tree_visitor(&temp_allocator);
  if (OB_FAIL(ret) || is_null_res) {
    // do nothing
  } else if (OB_FAIL(get_basic_type(geo1, basic_type))) {
    LOG_WARN("fail to get basic type", K(ret));
  } else if (OB_FAIL(geo1->do_visit(tree_visitor))) {
    LOG_WARN("failed to transform gc to tree", K(ret));
  } else if (FALSE_IT(geo_tree = tree_visitor.get_geometry())) {
  } else if (OB_FAIL(split_geo_to_basic_type(
                 geo_tree, temp_allocator, basic_type, split_geo))) {  // split_geo: ObCartesian*
    LOG_WARN("fail to split geometry to basic type", K(ret), K(basic_type));
  } else if (OB_FAIL(ObGeoExprUtils::check_empty(split_geo, is_geo_empty))) {
    LOG_WARN("check geo empty failed", K(ret));
  } else if (is_geo_empty) {
    is_null_res = true;
  } else if (OB_FAIL(affine_to_tile_space(split_geo, bounds, extent))) {
    LOG_WARN("fail to affine geometry", K(ret), K(extent));
  } else if (OB_FAIL(snap_geometry_to_grid(split_geo, temp_allocator, false))) {
    LOG_WARN("fail to do snap geometry", K(ret));
  } else if (OB_FAIL(ObGeoMVTUtil::simplify_geometry(split_geo))) {
    LOG_WARN("fail to simplify geometry", K(ret));
  } else if (OB_FAIL(ObGeoExprUtils::check_empty(split_geo, is_geo_empty))) {
    LOG_WARN("check geo empty failed", K(ret));
  } else if (is_geo_empty) {
    is_null_res = true;
  } else if (OB_FAIL(clip_geometry(split_geo,
                 temp_allocator,
                 basic_type,
                 extent,
                 buffer,
                 clip_geom,
                 is_null_res,
                 res_geo))) {
    LOG_WARN("fail to clip geometry", K(ret));
  } else if (OB_FAIL((ObGeoFuncUtils::simplify_multi_geo<ObCartesianGeometrycollection>(
              res_geo, temp_allocator)))) {
    LOG_WARN("fail to simplify multi geometry", K(ret));
  } else if (OB_FAIL(ObGeoExprUtils::check_empty(res_geo, is_geo_empty))) {
    LOG_WARN("check geo empty failed", K(ret));
  } else if (is_geo_empty) {
    is_null_res = true;
  }

  if (OB_SUCC(ret)) {
    if (is_null_res) {
      res.set_null();
    } else {
      ObGeometry *res_bin = nullptr;
      if (OB_FAIL(ObGeoTypeUtil::tree_to_bin(temp_allocator, res_geo, res_bin, nullptr))) {
        LOG_WARN("fail to convert tree to bin", K(ret));
      } else if (FALSE_IT(res_bin->set_srid(geo1->get_srid()))) {
      } else if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(
                     *res_bin, expr, ctx, nullptr, res_wkb, geo1->get_srid()))) {
        LOG_WARN("fail to get wkb from geometry", K(ret));
      } else {
        res.set_string(res_wkb);
      }
    }
  }
  return ret;
}

int ObExprPrivSTAsMVTGeom::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_asmvtgeom;
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase