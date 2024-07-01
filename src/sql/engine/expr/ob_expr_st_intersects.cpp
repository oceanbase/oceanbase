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
 * This file contains implementation for eval_st_intersects.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_ibin.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "ob_expr_st_intersects.h"
#include "lib/geo/ob_geo_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprSTIntersects::ObExprSTIntersects(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_INTERSECTS, N_ST_INTERSECTS, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTIntersects::~ObExprSTIntersects()
{
}

int ObExprSTIntersects::calc_result_type2(ObExprResType &type,
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
  type.set_int32();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  return ret;
}

int ObExprSTIntersects::eval_st_intersects(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  //int64_t start_time = common::ObTimeUtility::current_time();
  ObDatum *gis_datum1 = NULL;
  ObDatum *gis_datum2 = NULL;
  ObExpr *gis_arg1 = expr.args_[0];
  ObExpr *gis_arg2 = expr.args_[1];
  int num_args = expr.arg_cnt_;
  ObObjType input_type1 = gis_arg1->datum_meta_.type_;
  ObObjType input_type2 = gis_arg2->datum_meta_.type_;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  bool inter_result = false;
  if (OB_FAIL(gis_arg1->eval(ctx, gis_datum1)) || OB_FAIL(gis_arg2->eval(ctx, gis_datum2))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (gis_datum1->is_null() || gis_datum2->is_null()) {
    res.set_null();
  } else {
    ObGeoConstParamCache* const_param_cache = ObGeoExprUtils::get_geo_constParam_cache(expr.expr_ctx_id_, &ctx.exec_ctx_);
    bool is_geo1_empty = false;
    bool is_geo2_empty = false;
    ObGeometry *geo1 = NULL;
    ObGeometry *geo2 = NULL;
    ObGeoType type1;
    ObGeoType type2;
    uint32_t srid1;
    uint32_t srid2;
    ObString wkb1 = gis_datum1->get_string();
    ObString wkb2 = gis_datum2->get_string();
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    bool is_geo1_cached = false;
    bool is_geo2_cached = false;
    ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    bool box_intersects = true;

    if (gis_arg1->is_static_const_) {
      ObGeoExprUtils::expr_get_const_param_cache(const_param_cache, geo1, srid1, is_geo1_cached, 0);
    }
    if (gis_arg2->is_static_const_) {
      ObGeoExprUtils::expr_get_const_param_cache(const_param_cache, geo2, srid2, is_geo2_cached, 1);
    }
    if (!is_geo1_cached && OB_FAIL(ObGeoExprUtils::expr_prepare_build_geometry(temp_allocator, *gis_datum1, *gis_arg1, wkb1, type1, srid1))) {
      if (ret == OB_ERR_GIS_INVALID_DATA) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_INTERSECTS);
      }
    } else if (!is_geo2_cached && OB_FAIL(ObGeoExprUtils::expr_prepare_build_geometry(temp_allocator, *gis_datum2, *gis_arg2, wkb2, type2, srid2))) {
      if (ret == OB_ERR_GIS_INVALID_DATA) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_INTERSECTS);
      }
    } else if (srid1 != srid2) {
      LOG_WARN("srid not the same", K(srid1), K(srid2));
      ret = OB_ERR_GIS_DIFFERENT_SRIDS;
    } else if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;

      LOG_WARN("failed to get session", K(ret));
    } else if (!is_geo1_cached && !is_geo2_cached && OB_FAIL(ObGeoExprUtils::get_srs_item(session->get_effective_tenant_id(), srs_guard, srid1, srs))) {
      LOG_WARN("fail to get srs item", K(ret), K(srid1));
    } else if (!is_geo1_cached && OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb1, geo1, nullptr, N_ST_INTERSECTS, ObGeoBuildFlag::GEO_ALLOW_3D_CARTESIAN))) {
      LOG_WARN("get first geo by wkb failed", K(ret));
    } else if (!is_geo2_cached && OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb2, geo2, nullptr, N_ST_INTERSECTS, ObGeoBuildFlag::GEO_ALLOW_3D_CARTESIAN))) {
      LOG_WARN("get second geo by wkb failed", K(ret));
    } else if ((!is_geo1_cached && OB_FAIL(ObGeoExprUtils::check_empty(geo1, is_geo1_empty)))
        || (!is_geo2_cached && OB_FAIL(ObGeoExprUtils::check_empty(geo2, is_geo2_empty)))) {
      LOG_WARN("check geo empty failed", K(ret));
    } else if (is_geo1_empty || is_geo2_empty) {
      res.set_null();
    } else if (OB_FAIL(ObGeoExprUtils::zoom_in_geos_for_relation(*geo1, *geo2, is_geo1_cached, is_geo2_cached))) {
      LOG_WARN("zoom in geos failed", K(ret));
    } else {
      if (OB_NOT_NULL(const_param_cache)) {
        if (gis_arg1->is_static_const_ && !is_geo1_cached &&
            OB_FAIL(const_param_cache->add_const_param_cache(0, *geo1))) {
          LOG_WARN("add geo1 to const cache failed", K(ret));
        } else if (gis_arg2->is_static_const_ && !is_geo2_cached &&
            OB_FAIL(const_param_cache->add_const_param_cache(1, *geo2))) {
          LOG_WARN("add geo2 to const cache failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObGeoExprUtils::get_intersects_res(*geo1, *geo2, gis_arg1, gis_arg2,
                                                            const_param_cache, srs,
                                                            temp_allocator, inter_result))) {
        LOG_WARN("fail to get intersects res", K(ret));
      } else {
        res.set_bool(inter_result);
      }
    }
  }
  return ret;
}

int ObExprSTIntersects::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_intersects;
  return OB_SUCCESS;
}

}
}
