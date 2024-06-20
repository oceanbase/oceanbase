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
 * This file contains implementation for eval_st_within.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_ibin.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "ob_expr_st_within.h"
#include "lib/geo/ob_geo_cache.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSTWithin::ObExprSTWithin(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_WITHIN, N_ST_WITHIN, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTWithin::~ObExprSTWithin()
{
}

int ObExprSTWithin::calc_result_type2(ObExprResType &type,
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

int ObExprSTWithin::eval_st_within(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum1 = NULL;
  ObDatum *gis_datum2 = NULL;
  ObExpr *gis_arg1 = expr.args_[0];
  ObExpr *gis_arg2 = expr.args_[1];
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
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
    } else if (!is_geo1_cached && OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb1, geo1, nullptr, N_ST_WITHIN, ObGeoBuildFlag::GEO_ALLOW_3D_CARTESIAN))) {
      LOG_WARN("get first geo by wkb failed", K(ret));
    } else if (!is_geo2_cached && OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb2, geo2, nullptr, N_ST_WITHIN, ObGeoBuildFlag::GEO_ALLOW_3D_CARTESIAN))) {
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
      } else {
        ObGeoEvalCtx gis_context(&temp_allocator);
        bool result = false;
        if (OB_FAIL(gis_context.append_geo_arg(geo1)) || OB_FAIL(gis_context.append_geo_arg(geo2))) {
          LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
        } else {
          ObCachedGeom *cache_geo = NULL;
          ObGeometry *geo;
          if (OB_NOT_NULL(const_param_cache)) {
            if (gis_arg2->is_static_const_) {
              cache_geo = const_param_cache->get_cached_geo(1);
              if (cache_geo == NULL
                && OB_FAIL(ObGeoTypeUtil::create_cached_geometry(*const_param_cache->get_allocator(),
                                                                  temp_allocator,
                                                                  const_param_cache->get_const_param_cache(1),
                                                                  srs,
                                                                  cache_geo))) {
                LOG_WARN("add geo2 to const cache failed", K(ret));
              } else {
                geo = geo1;
                const_param_cache->add_cached_geo(1, cache_geo);
              }
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_NOT_NULL(cache_geo)) {
            if (OB_FAIL(cache_geo->contains(*geo, gis_context, result))) {
              LOG_WARN("get contains result failed", K(ret));
            } else {
              res.set_bool(result);
            }
          } else if (ObGeoTypeUtil::use_point_polygon_short_circuit(*geo1, *geo2, T_FUN_SYS_ST_WITHIN)) {
            bool result = false;
            if (OB_FAIL(ObGeoTypeUtil::get_point_polygon_res(geo1, geo2, T_FUN_SYS_ST_WITHIN, result))) {
              LOG_WARN("fail to get res.", K(ret));
            } else {
              res.set_bool(result);
            }
          } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Within>::gis_func::eval(gis_context, result))) {
            LOG_WARN("eval st intersection failed", K(ret));
            ObGeoExprUtils::geo_func_error_handle(ret, N_ST_WITHIN);
          } else {
            res.set_bool(result);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprSTWithin::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_within;
  return OB_SUCCESS;
}

}
}
