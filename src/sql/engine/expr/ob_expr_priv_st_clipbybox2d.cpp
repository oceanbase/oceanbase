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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_clipbybox2d.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_utils.h"
#include "share/object/ob_obj_cast_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTClipByBox2D::ObExprPrivSTClipByBox2D(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_CLIPBYBOX2D, N_PRIV_ST_CLIPBYBOX2D, 2,
        NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprPrivSTClipByBox2D::~ObExprPrivSTClipByBox2D()
{}

int ObExprPrivSTClipByBox2D::calc_result_type2(ObExprResType &type, ObExprResType &type1,
    ObExprResType &type2, common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjType obj_type1 = type1.get_type();
  ObObjType obj_type2 = type2.get_type();

  if (!ob_is_string_type(obj_type1) && !ob_is_geometry(obj_type1) && !ob_is_null(obj_type1)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_CLIPBYBOX2D);
    LOG_WARN("invalid type", K(ret), K(obj_type1));
  } else if (!ob_is_string_type(obj_type2) && !ob_is_geometry(obj_type2)
             && !ob_is_null(obj_type2)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_CLIPBYBOX2D);
    LOG_WARN("invalid type", K(ret), K(obj_type2));
  } else {
    ObCastMode cast_mode = type_ctx.get_cast_mode();
    cast_mode &= ~CM_WARN_ON_FAIL;      // make cast return error when fail
    type_ctx.set_cast_mode(cast_mode);  // cast mode only do work in new sql engine cast frame.
    type.set_geometry();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  }

  return ret;
}

int ObExprPrivSTClipByBox2D::process_input_geometry(omt::ObSrsCacheGuard &srs_guard, const ObExpr &expr, ObEvalCtx &ctx, ObIAllocator &allocator,
    bool &is_null_res, ObGeometry *&geo1, ObGeometry *&geo2, const ObSrsItem *&srs1,
    const ObSrsItem *&srs2)
{
  int ret = OB_SUCCESS;
  ObDatum *datum1 = nullptr;
  ObDatum *datum2 = nullptr;
  ObExpr *arg1 = expr.args_[0];
  ObExpr *arg2 = expr.args_[1];
  ObObjType type1 = arg1->datum_meta_.type_;
  ObObjType type2 = arg2->datum_meta_.type_;

  if (ob_is_null(type1) || ob_is_null(type2)) {
    is_null_res = true;
  } else if (OB_FAIL(arg1->eval(ctx, datum1)) || OB_FAIL(arg2->eval(ctx, datum2))) {
    LOG_WARN("fail to eval args", K(ret));
  } else if (datum1->is_null() || datum2->is_null()) {
    is_null_res = true;
  } else {
    ObString wkb1 = datum1->get_string();
    ObString wkb2 = datum2->get_string();

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
            allocator, *datum1, arg1->datum_meta_, arg1->obj_meta_.has_lob_header(), wkb1))) {
      LOG_WARN(
          "fail to read real string data", K(ret), K(arg1->obj_meta_.has_lob_header()), K(wkb1));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
                   *datum2,
                   arg2->datum_meta_,
                   arg2->obj_meta_.has_lob_header(),
                   wkb2))) {
      LOG_WARN(
          "fail to read real string data", K(ret), K(arg2->obj_meta_.has_lob_header()), K(wkb2));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(
                   ctx, srs_guard, wkb1, srs1, true, N_PRIV_ST_CLIPBYBOX2D))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb1));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(
                   ctx, srs_guard, wkb2, srs2, true, N_PRIV_ST_CLIPBYBOX2D))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb2));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(
                   allocator, wkb1, geo1, nullptr, N_PRIV_ST_CLIPBYBOX2D))) {  // ObIWkbGeom
      LOG_WARN("fail to build geometry from wkb", K(ret), K(wkb1));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(
                   allocator, wkb2, geo2, nullptr, N_PRIV_ST_CLIPBYBOX2D))) {  // ObIWkbGeom
      LOG_WARN("fail to build geometry from wkb", K(ret), K(wkb2));
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
    }
  }
  return ret;
}

int ObExprPrivSTClipByBox2D::eval_priv_st_clipbybox2d(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_null_res = false;
  ObGeometry *geo1 = nullptr;
  ObGeometry *geo2 = nullptr;
  ObGeometry *res_geo = NULL;
  bool is_geo1_empty = false;
  bool is_geo2_empty = false;
  const ObSrsItem *srs1 = nullptr;
  const ObSrsItem *srs2 = nullptr;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObString res_wkb;
  omt::ObSrsCacheGuard srs_guard;

  if (OB_FAIL(process_input_geometry(srs_guard, expr, ctx, temp_allocator, is_null_res, geo1, geo2, srs1, srs2))) {
    LOG_WARN("fail to process input geometry", K(ret), K(geo1), K(geo2), K(is_null_res));
  } else if (is_null_res) {
    // do nothing
  } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo1, is_geo1_empty))
             || OB_FAIL(ObGeoExprUtils::check_empty(geo2, is_geo2_empty))) {
    LOG_WARN("check geo empty failed", K(ret));
  } else if (is_geo2_empty) {
    is_null_res = true;
  } else if (is_geo1_empty) {
    // return empty when first geo argument is empty
    res_geo = geo1;
  } else {
    ObGeoEvalCtx box_ctx(&temp_allocator);
    box_ctx.set_is_called_in_pg_expr(true);
    ObGeogBox *gbox = nullptr;
    // calculate 2d box of geo2, then convert the box to a rectangle geo
    if (OB_FAIL(box_ctx.append_geo_arg(geo2))) {
      LOG_WARN("build gis context failed", K(ret), K(box_ctx.get_geo_count()));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(box_ctx, gbox))) {
      LOG_WARN("failed to do box functor failed", K(ret));
    } else if (OB_FAIL(ObGeoBoxUtil::clip_by_box(*geo1, temp_allocator, *gbox, res_geo, true))) {
      LOG_WARN("fail to do clip by box", K(ret));
    } else if (OB_ISNULL(res_geo)) {
      is_null_res = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null_res) {
      res.set_null();
    } else {
      if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*res_geo, expr, ctx, srs1, res_wkb))) {
        LOG_WARN("fail to get wkb from geometry", K(ret));
      } else {
        res.set_string(res_wkb);
      }
    }
  }
  return ret;
}

int ObExprPrivSTClipByBox2D::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_clipbybox2d;
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase