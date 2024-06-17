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
 * This file contains implementation for ob_expr_st_area.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_st_area.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{

ObExprSTArea::ObExprSTArea(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_AREA, N_ST_AREA, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTArea::~ObExprSTArea()
{
}

int ObExprSTArea::calc_result_type1(ObExprResType &type,
                                    ObExprResType &type1,
                                    common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (ob_is_numeric_type(type1.get_type())) {
    type1.set_calc_type(ObLongTextType);
  } else if (!ob_is_geometry(type1.get_type())
             && !ob_is_string_type(type1.get_type())
             && type1.get_type() != ObNullType) {
    // handle string types as hex strings(wkb)
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_AREA);
    LOG_WARN("invalid type", K(ret), K(type1.get_type()));
  } else {
    type.set_double();
  }
  return ret;
}

int ObExprSTArea::eval_st_area(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum = NULL;
  ObExpr *gis_arg = expr.args_[0];
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObObjType input_type = gis_arg->datum_meta_.type_;

  if (OB_FAIL(gis_arg->eval(ctx, gis_datum))) {
    LOG_WARN("eval geo arg failed", K(ret));
  } else if (gis_datum->is_null()) {
    res.set_null();
  } else {
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObString wkb = gis_datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum,
        gis_arg->datum_meta_, gis_arg->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs, true, N_ST_AREA))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb, geo, srs, N_ST_AREA, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {
      LOG_WARN("get geo by wkb failed", K(ret));
    } else if (geo->type() != ObGeoType::POLYGON && geo->type() != ObGeoType::MULTIPOLYGON) {
      ret = OB_ERR_UNEXPECTED_GEOMETRY_TYPE;
      LOG_WARN("unexpected geometry type for st_area", K(ret));
      LOG_USER_ERROR(OB_ERR_UNEXPECTED_GEOMETRY_TYPE, "POLYGON/MULTIPOLYGON",
        ObGeoTypeUtil::get_geo_name_by_type(geo->type()), N_ST_AREA);
    } else {
      ObGeoEvalCtx gis_context(&temp_allocator, srs);
      int correct_result;
      double result = 0.0;
      if (OB_FAIL(gis_context.append_geo_arg(geo))) {
        LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Area>::geo_func::eval(gis_context, result))) {
        LOG_WARN("eval st area failed", K(ret));
        ObGeoExprUtils::geo_func_error_handle(ret, N_ST_AREA);
      } else if (!std::isfinite(result)) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("Result value is out of range in st_area", K(ret));
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "Result", N_ST_AREA);
      } else {
        res.set_double(result);
      }
    }
  }
  return ret;
}

int ObExprSTArea::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_area;
  return OB_SUCCESS;
}

}
}
