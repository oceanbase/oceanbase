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
 * This file contains implementation for eval_st_covers.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_ibin.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "ob_expr_st_covers.h"
#include "lib/geo/ob_geo_cache.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprPrivSTCovers::ObExprPrivSTCovers(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_COVERS, N_PRIV_ST_COVERS, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprPrivSTCovers::~ObExprPrivSTCovers()
{
}

int ObExprPrivSTCovers::calc_result_type2(ObExprResType &type,
                                          ObExprResType &type1,
                                          ObExprResType &type2,
                                          common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (!ob_is_geometry(type1.get_type()) && !ob_is_string_type(type1.get_type()) && type1.get_type() != ObNullType) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid type", K(ret), K(type1.get_type()));
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_COVERS);
  } else if (!ob_is_geometry(type2.get_type()) && !ob_is_string_type(type2.get_type()) && type2.get_type() != ObNullType) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid type", K(ret), K(type2.get_type()));
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_COVERS);
  }
  if (OB_SUCC(ret)) {
    type.set_int32();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }
  return ret;
}

template<typename ResType>
int ObExprPrivSTCovers::eval_st_covers_common(const ObExpr &expr, ObEvalCtx &ctx,
                                              ObArenaAllocator &temp_allocator,
                                              ObString wkb1,
                                              ObString wkb2,
                                              ResType &res)
{
  int ret = OB_SUCCESS;
  ObGeometry *geo1 = NULL;
  ObGeometry *geo2 = NULL;
  ObGeoType type1;
  ObGeoType type2;
  uint32_t srid1;
  uint32_t srid2;
  bool is_geo1_empty = false;
  bool is_geo2_empty = false;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = NULL;
  bool is_geo1_cached = false;
  bool is_geo2_cached = false;
  ObExpr *gis_arg1 = expr.args_[0];
  ObExpr *gis_arg2 = expr.args_[1];
  ObGeoConstParamCache* const_param_cache = ObGeoExprUtils::get_geo_constParam_cache(expr.expr_ctx_id_, &ctx.exec_ctx_);
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_NOT_NULL(const_param_cache) && gis_arg1->is_static_const_) {
    geo1 = const_param_cache->get_const_param_cache(0);
    if (geo1 != NULL) {
      srid1 = geo1->get_srid();
      is_geo1_cached = true;
    }
  }
  if (OB_NOT_NULL(const_param_cache) && gis_arg2->is_static_const_) {
    geo2 = const_param_cache->get_const_param_cache(1);
    if (geo2 != NULL) {
      srid2 = geo2->get_srid();
      is_geo2_cached = true;
    }
  }

  if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb1, type1, srid1))) {
    LOG_WARN("get type and srid from wkb failed", K(wkb1), K(ret));
  } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb2, type2, srid2))) {
    LOG_WARN("get type and srid from wkb failed", K(wkb2), K(ret));
  } else if (srid1 != srid2) {
    LOG_WARN("srid not the same", K(srid1), K(srid2));
    ret = OB_ERR_GIS_DIFFERENT_SRIDS;
    LOG_USER_ERROR(OB_ERR_GIS_DIFFERENT_SRIDS, N_PRIV_ST_COVERS, srid1, srid2);
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get session", K(ret));
  } else if (!is_geo1_cached && !is_geo2_cached && OB_FAIL(ObGeoExprUtils::get_srs_item(session->get_effective_tenant_id(), srs_guard, srid1, srs))) {
    LOG_WARN("fail to get srs item", K(ret), K(srid1));
  } else if (ObGeoTypeUtil::is_geo1_dimension_higher_than_geo2(type2, type1)) {
    res.set_bool(false);
  } else if (!is_geo1_cached && OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb1, geo1, nullptr, N_PRIV_ST_COVERS,
                                                    ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT | ObGeoBuildFlag::GEO_CHECK_RING))) {
    LOG_WARN("get first geo by wkb failed", K(ret));
  } else if (!is_geo2_cached && OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb2, geo2, nullptr, N_PRIV_ST_COVERS,
                                                    ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT | ObGeoBuildFlag::GEO_CHECK_RING))) {
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
      if (OB_FAIL(gis_context.append_geo_arg(geo2)) || OB_FAIL(gis_context.append_geo_arg(geo1))) {
        LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
      } else {
        ObCachedGeom *cache_geo = NULL;
        ObGeometry *geo;
        if (OB_NOT_NULL(const_param_cache)) {
          if (gis_arg1->is_static_const_) {
            cache_geo = const_param_cache->get_cached_geo(0);
            if (cache_geo == NULL
              && OB_FAIL(ObGeoTypeUtil::create_cached_geometry(*const_param_cache->get_allocator(),
                                                                temp_allocator,
                                                                const_param_cache->get_const_param_cache(0),
                                                                srs,
                                                                cache_geo))) {
              LOG_WARN("add geo2 to const cache failed", K(ret));
            } else {
              geo = geo2;
              const_param_cache->add_cached_geo(0, cache_geo);
            }
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_NOT_NULL(cache_geo)) {
          if (OB_FAIL(cache_geo->cover(*geo, gis_context, result))) {
            LOG_WARN("get contains result failed", K(ret));
          } else {
            res.set_bool(result);
          }
        } else if (ObGeoTypeUtil::use_point_polygon_short_circuit(*geo1, *geo2, T_FUN_SYS_ST_COVERS)) {
          bool result = false;
          if (OB_FAIL(ObGeoTypeUtil::get_point_polygon_res(geo1, geo2, T_FUN_SYS_ST_COVERS, result))) {
            LOG_WARN("fail to get res.", K(ret));
          } else {
            res.set_bool(result);
          }
        } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::CoveredBy>::geo_func::eval(gis_context, result))) {
          LOG_WARN("eval st coveredBy failed", K(ret));
          ObGeoExprUtils::geo_func_error_handle(ret, N_PRIV_ST_COVERS);
        } else {
          res.set_bool(result);
        }
      }
    }
  }
  return ret;
}

int ObExprPrivSTCovers::eval_st_covers(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum1 = NULL;
  ObDatum *gis_datum2 = NULL;
  ObString wkb1;
  ObString wkb2;
  ObExpr *gis_arg1 = expr.args_[0];
  ObExpr *gis_arg2 = expr.args_[1];
  ObObjType input_type1 = gis_arg1->datum_meta_.type_;
  ObObjType input_type2 = gis_arg2->datum_meta_.type_;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (OB_FAIL(gis_arg1->eval(ctx, gis_datum1)) || OB_FAIL(gis_arg2->eval(ctx, gis_datum2))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (gis_datum1->is_null() || gis_datum2->is_null()) {
    res.set_null();
  } else if (FALSE_IT(wkb1 = gis_datum1->get_string())) {
  } else if (FALSE_IT(wkb2 = gis_datum2->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum1,
            gis_arg1->datum_meta_, gis_arg1->obj_meta_.has_lob_header(), wkb1))) {
    LOG_WARN("fail to get real string data", K(ret), K(wkb1));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum2,
            gis_arg2->datum_meta_, gis_arg2->obj_meta_.has_lob_header(), wkb2))) {
    LOG_WARN("fail to get real string data", K(ret), K(wkb2));
  } else if (OB_FAIL(ObExprPrivSTCovers::eval_st_covers_common(expr, ctx,
                                                               temp_allocator,
                                                               wkb1,
                                                               wkb2,
                                                               res))) {
    LOG_WARN("eval st covers failed", K(ret));
  }
  return ret;
}

int ObExprPrivSTCovers::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_covers;
  return OB_SUCCESS;
}

}
}
