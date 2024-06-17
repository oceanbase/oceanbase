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
 * This file contains implementation for eval_priv_st_equals.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_ibin.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_expr_priv_st_equals.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprPrivSTEquals::ObExprPrivSTEquals(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_EQUALS, N_PRIV_ST_EQUALS, 2,
          VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprPrivSTEquals::~ObExprPrivSTEquals()
{}

int ObExprPrivSTEquals::calc_result_type2(ObExprResType &type, ObExprResType &type1,
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
  type.set_int();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  return ret;
}

int ObExprPrivSTEquals::get_input_geometry(omt::ObSrsCacheGuard &srs_guard, ObDatum *gis_datum, ObEvalCtx &ctx, ObExpr *gis_arg, bool &is_null_geo,
    const ObSrsItem *&srs, ObGeometry *&geo, bool &is_geo_empty)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard ctx_alloc_g(ctx);
  common::ObArenaAllocator &allocator = ctx_alloc_g.get_allocator();
  if (OB_FAIL(gis_arg->eval(ctx, gis_datum))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (gis_datum->is_null()) {
    is_null_geo = true;
  } else {
    ObString wkb = gis_datum->get_string();
    ObGeoType type = ObGeoType::GEOTYPEMAX;
    uint32_t srid = -1;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
            *gis_datum,
            gis_arg->datum_meta_,
            gis_arg->obj_meta_.has_lob_header(),
            wkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb, type, srid))) {
      if (ret == OB_ERR_GIS_INVALID_DATA) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_EQUALS);
      }
      LOG_WARN("get type and srid from wkb failed", K(wkb), K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(
                   ctx, srs_guard, wkb, srs, true, N_PRIV_ST_EQUALS))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(allocator,
                   wkb,
                   geo,
                   srs,
                   N_PRIV_ST_EQUALS,
                   ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {
      LOG_WARN("get first geo by wkb failed", K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo, is_geo_empty))) {
      LOG_WARN("check geo empty failed", K(ret));
    }
  }
  return ret;
}

int ObExprPrivSTEquals::eval_priv_st_equals(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObExpr *gis_arg1 = expr.args_[0];
  ObExpr *gis_arg2 = expr.args_[1];
  bool is_geo1_empty = false;
  bool is_geo2_empty = false;
  bool is_geo1_null = false;
  bool is_geo2_null = false;
  ObGeometry *geo1 = nullptr;
  ObGeometry *geo2 = nullptr;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs1 = nullptr;
  const ObSrsItem *srs2 = nullptr;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObDatum *gis_datum1 = nullptr;
  ObDatum *gis_datum2 = nullptr;
  if (OB_FAIL(gis_arg1->eval(ctx, gis_datum1)) || OB_FAIL(gis_arg2->eval(ctx, gis_datum2))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (gis_datum1->is_null() || gis_datum2->is_null()) {
    res.set_null();
  } else if (OB_FAIL(get_input_geometry(srs_guard, gis_datum1, ctx, gis_arg1, is_geo1_null, srs1, geo1, is_geo1_empty))) {
    LOG_WARN("fail to get input geometry", K(ret));
  } else if (OB_FAIL(get_input_geometry(srs_guard, gis_datum2, ctx, gis_arg2, is_geo2_null, srs2, geo2, is_geo2_empty))) {
    LOG_WARN("fail to get input geometry", K(ret));
  } else {
    uint32_t srid1 = srs1 == nullptr ? 0 : srs1->get_srid();
    uint32_t srid2 = srs2 == nullptr ? 0 : srs2->get_srid();
    if (srid1 != srid2) {
      ret = OB_ERR_GIS_DIFFERENT_SRIDS;
      LOG_WARN("srid not the same", K(ret), K(srid1), K(srid2));
      LOG_USER_ERROR(OB_ERR_GIS_DIFFERENT_SRIDS, N_PRIV_ST_EQUALS, srid1, srid2);
    } else if (is_geo1_empty || is_geo2_empty) {
      res.set_bool(is_geo1_empty && is_geo2_empty);
    } else if (OB_FAIL(ObGeoExprUtils::zoom_in_geos_for_relation(*geo1, *geo2))) {
      LOG_WARN("zoom in geos failed", K(ret));
    } else {
      bool result = false;
      ObGeoEvalCtx gis_context(&temp_allocator, srs1);
      if (OB_FAIL(gis_context.append_geo_arg(geo1)) || OB_FAIL(gis_context.append_geo_arg(geo2))) {
        LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Equals>::geo_func::eval(gis_context, result))) {
        LOG_WARN("eval st intersection failed", K(ret));
        ObGeoExprUtils::geo_func_error_handle(ret, N_PRIV_ST_EQUALS);
      } else {
        res.set_bool(result);
      }
    }
  }
  return ret;
}

int ObExprPrivSTEquals::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_equals;
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
