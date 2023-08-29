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
 * This file contains implementation for eval_st_contains.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_ibin.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "ob_expr_st_contains.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSTContains::ObExprSTContains(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_CONTAINS, N_ST_CONTAINS, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTContains::~ObExprSTContains()
{
}

int ObExprSTContains::calc_result_type2(ObExprResType &type,
                                        ObExprResType &type1,
                                        ObExprResType &type2,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  int unexpected_types = 0;
  int null_types = 0;

  if (type1.get_type() == ObNullType) {
    null_types++;
  } else if (!ob_is_geometry(type1.get_type()) && !ob_is_string_type(type1.get_type())) {
    unexpected_types++;
    LOG_WARN("invalid type", K(type1.get_type()));
  }
  if (type2.get_type() == ObNullType) {
    null_types++;
  } else if (!ob_is_geometry(type2.get_type()) && !ob_is_string_type(type2.get_type())) {
    unexpected_types++;
    LOG_WARN("invalid type", K(type2.get_type()));
  }
  // an invalid type and a null type will return null
  // an invalid type and a valid type return error
  if (null_types == 0 && unexpected_types > 0) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_CONTAINS);
    LOG_WARN("invalid type", K(ret));
  }
  if (OB_SUCC(ret)) {
    type.set_int32();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }
  return ret;
}

int ObExprSTContains::eval_st_contains(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
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

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum1,
              gis_arg1->datum_meta_, gis_arg1->obj_meta_.has_lob_header(), wkb1))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb1));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum2,
              gis_arg2->datum_meta_, gis_arg2->obj_meta_.has_lob_header(), wkb2))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb2));
    } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb1, type1, srid1))) {
      if (ret == OB_ERR_GIS_INVALID_DATA) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_CONTAINS);
      }
      LOG_WARN("get type and srid from wkb failed", K(wkb1), K(ret));
    } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb2, type2, srid2))) {
      LOG_WARN("get type and srid from wkb failed", K(wkb2), K(ret));
    } else if (srid1 != srid2) {
      LOG_WARN("srid not the same", K(srid1), K(srid2));
      ret = OB_ERR_GIS_DIFFERENT_SRIDS;
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb1, srs, true, N_ST_CONTAINS))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb1));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb1, geo1, srs, N_ST_CONTAINS))) {
      LOG_WARN("get first geo by wkb failed", K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb2, geo2, srs, N_ST_CONTAINS))) {
      LOG_WARN("get second geo by wkb failed", K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo1, is_geo1_empty))
        || OB_FAIL(ObGeoExprUtils::check_empty(geo2, is_geo2_empty))) {
      LOG_WARN("check geo empty failed", K(ret));
    } else if (is_geo1_empty || is_geo2_empty) {
      res.set_null();
    } else if (OB_FAIL(ObGeoExprUtils::zoom_in_geos_for_relation(*geo1, *geo2))) {
      LOG_WARN("zoom in geos failed", K(ret));
    } else {
      ObGeoEvalCtx gis_context(&temp_allocator, srs);
      bool result = false;
      if (OB_FAIL(gis_context.append_geo_arg(geo2)) || OB_FAIL(gis_context.append_geo_arg(geo1))) {
        LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Within>::gis_func::eval(gis_context, result))) {
        LOG_WARN("eval Within functor failed", K(ret));
        ObGeoExprUtils::geo_func_error_handle(ret, N_ST_CONTAINS);
      } else {
        res.set_bool(result);
      }
    }
  }
  return ret;
}

int ObExprSTContains::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_contains;
  return OB_SUCCESS;
}

}
}
