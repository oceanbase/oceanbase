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

#include "object/ob_obj_type.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "ob_expr_st_pointn.h"
#include "lib/geo/ob_geo_pointn_visitor.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSTPointN::ObExprSTPointN(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_POINTN, N_ST_POINTN, 2,
          VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprSTPointN::~ObExprSTPointN()
{}

int ObExprSTPointN::calc_result_type2(ObExprResType &type, ObExprResType &type1,
    ObExprResType &type2, common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  ObObjType type_g = type1.get_type();
  ObObjType type_n = type2.get_type();

  if (ob_is_null(type_g)) {
    // do nothing
  } else if (ob_is_numeric_type(type_g)) {
    type1.set_calc_type(ObLongTextType);
  } else if (!ob_is_geometry(type_g) && !ob_is_string_type(type_g)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_name());
  }

  if (OB_SUCC(ret)) {
    if (ob_is_null(type_n)) {
      // do nothing
    } else {
      type2.set_calc_type(ObIntType);
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
    }
  }
  type.set_geometry();
  type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  return ret;
}

int ObExprSTPointN::eval_st_pointn(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_null_res = false;
  ObDatum *gis_datum1 = nullptr;
  ObDatum *gis_datum2 = nullptr;
  ObExpr *gis_arg1 = expr.args_[0];
  ObExpr *gis_arg2 = expr.args_[1];

  ObGeometry *pointn_res = nullptr;
  const ObSrsItem *srs = NULL;
  bool is_empty_res = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();

  if (OB_FAIL(gis_arg1->eval(ctx, gis_datum1)) || OB_FAIL(gis_arg2->eval(ctx, gis_datum2))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (gis_datum1->is_null() || gis_datum2->is_null()) {
    is_null_res = true;
  } else {
    ObGeometry *geo = nullptr;
    omt::ObSrsCacheGuard srs_guard;
    // ObGeoBoostAllocGuard guard(tenant_id);
    ObString wkb = gis_datum1->get_string();

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator,
            *gis_datum1,
            gis_arg1->datum_meta_,
            gis_arg1->obj_meta_.has_lob_header(),
            wkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(
                   ctx, srs_guard, wkb, srs, true, N_ST_POINTN))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator,
                   wkb,
                   geo,
                   srs,
                   N_ST_POINTN,
                   ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT | GEO_RESERVE_3D))) {
      LOG_WARN("get first geo by wkb failed", K(ret));
    } else if (geo->type() != ObGeoType::LINESTRING) {
      is_null_res = true;
    } else {
      ObGeoPointNVisitor geom_visitor(&temp_allocator, gis_datum2->get_int());
      if (OB_FAIL(geo->do_visit(geom_visitor))) {
        LOG_WARN("failed to find st_pointn", K(ret));
      } else if (OB_ISNULL(pointn_res = geom_visitor.get_point())) {
        is_null_res = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    res.set_null();
  } else {
    ObString res_wkb;
    if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*pointn_res, expr, ctx, srs, res_wkb))) {
      LOG_WARN("failed to write geometry to wkb", K(ret));
    } else {
      res.set_string(res_wkb);
    }
  }
  
  return ret;
}

int ObExprSTPointN::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_pointn;
  return OB_SUCCESS;
}


}  // namespace sql
}  // namespace oceanbase
