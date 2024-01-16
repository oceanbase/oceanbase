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
 * This file contains implementation for _st_pointonsurface.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_pointonsurface.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/omt/ob_tenant_srs.h"
#include "lib/geo/ob_geo_func_register.h"
#include "share/object/ob_obj_cast_util.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "lib/geo/ob_geo_interior_point_visitor.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTPointOnSurface::ObExprPrivSTPointOnSurface(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_POINTONSURFACE, N_PRIV_ST_POINTONSURFACE, 1,
        NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprPrivSTPointOnSurface::~ObExprPrivSTPointOnSurface()
{}

int ObExprPrivSTPointOnSurface::calc_result_type1(
    ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjType obj_type1 = type1.get_type();

  if (!ob_is_string_type(obj_type1) && !ob_is_geometry(obj_type1) && !ob_is_null(obj_type1)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_POINTONSURFACE);
    LOG_WARN("invalid type", K(ret), K(obj_type1));
  } else {
    ObCastMode cast_mode = type_ctx.get_cast_mode();
    cast_mode &= ~CM_WARN_ON_FAIL;      // make cast return error when fail
    type_ctx.set_cast_mode(cast_mode);  // cast mode only do work in new sql engine cast frame.
    type.set_geometry();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  }

  return ret;
}

int ObExprPrivSTPointOnSurface::process_input_geometry(
    const ObExpr &expr, ObEvalCtx &ctx, ObIAllocator &allocator, bool &is_null_res, ObGeometry *&geo1)
{
  int ret = OB_SUCCESS;
  ObDatum *datum1 = nullptr;
  ObExpr *arg1 = expr.args_[0];
  ObObjType type1 = arg1->datum_meta_.type_;

  if (ob_is_null(type1)) {
    is_null_res = true;
  } else if (OB_FAIL(arg1->eval(ctx, datum1))) {
    LOG_WARN("fail to eval args", K(ret));
  } else if (datum1->is_null()) {
    is_null_res = true;
  } else {
    ObString wkb1 = datum1->get_string();
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs1 = nullptr;

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
            allocator, *datum1, arg1->datum_meta_, arg1->obj_meta_.has_lob_header(), wkb1))) {
      LOG_WARN("fail to read real string data", K(ret), K(arg1->obj_meta_.has_lob_header()), K(wkb1));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb1, srs1, true, N_PRIV_ST_POINTONSURFACE))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb1));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(
                   allocator, wkb1, geo1, nullptr, N_PRIV_ST_POINTONSURFACE))) {  // ObIWkbGeom
      LOG_WARN("fail to build geometry from wkb", K(ret), K(wkb1));
    } else if (OB_NOT_NULL(srs1) && srs1->is_geographical_srs()) {
      ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
      LOG_USER_ERROR(OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS, N_PRIV_ST_ASMVTGEOM,
                  ObGeoTypeUtil::get_geo_name_by_type(geo1->type()));
      LOG_WARN("Geometry in geographical srs can not be input", K(ret), K(srs1));
    }
  }

  return ret;
}

int ObExprPrivSTPointOnSurface::eval_priv_st_pointonsurface(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_null_res = false;
  ObGeometry *geo1 = nullptr;
  ObGeometry *res_geo = NULL;
  ObGeometry *interior_point = nullptr;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObString res_wkb;

  if (OB_FAIL(process_input_geometry(expr, ctx, temp_allocator, is_null_res, geo1))) {
    LOG_WARN("fail to process input geometry", K(ret), K(geo1), K(is_null_res));
  } else if (is_null_res) {
    // do nothing
  } else {
    ObGeoInteriorPointVisitor inter_point_visitor(&temp_allocator);
    if (OB_FAIL(geo1->do_visit(inter_point_visitor))) {
      LOG_WARN("fail to do interior point visitor", K(ret));
    } else if (OB_FAIL(inter_point_visitor.get_interior_point(interior_point))) {
      LOG_WARN("fail to get interior point", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null_res) {
      res.set_null();
    } else {
      if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*interior_point, expr, ctx, nullptr, res_wkb, geo1->get_srid()))) {
        LOG_WARN("fail to get wkb from geometry", K(ret));
      } else {
        res.set_string(res_wkb);
      }
    }
  }

  return ret;
}

int ObExprPrivSTPointOnSurface::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_pointonsurface;
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase