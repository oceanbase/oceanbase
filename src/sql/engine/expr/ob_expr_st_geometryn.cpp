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
 * This file contains implementation for st_geometryn.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "ob_expr_st_geometryn.h"
#include "lib/geo/ob_srs_info.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprSTGeometryN::ObExprSTGeometryN(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_GEOMETRYN, N_ST_GEOMETRYN, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprSTGeometryN::calc_result_type2(ObExprResType &type,
                                         ObExprResType &type1,
                                         ObExprResType &type2,
                                         common::ObExprTypeCtx &type_ctx) const
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
    }
  }
  type.set_geometry();
  type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());

  return ret;
}

int ObExprSTGeometryN::eval_st_geometryn(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum = NULL;
  ObDatum *datum2 = NULL;
  ObGeometry *src_geo = NULL;
  ObGeometry *dest_geo = NULL;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  bool is_null_result = false;

  // check null
  is_null_result = ob_is_null(expr.args_[0]->datum_meta_.type_) || ob_is_null(expr.args_[1]->datum_meta_.type_);

  if (is_null_result) {
    res.set_null();
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, gis_datum))) {
    LOG_WARN("eval geo arg failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, datum2))) {
    LOG_WARN("eval index arg failed", K(ret));
  } else if (gis_datum->is_null() || datum2->is_null()) {
    res.set_null();
  } else {
    ObString wkb = gis_datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum,
              expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs, true, N_ST_GEOMETRYN))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb, src_geo, srs, 
                                                      N_ST_GEOMETRYN, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {
      LOG_WARN("failed to parse wkb", K(ret));        // ObIWkbGeom
    } else if ((src_geo->type() <= ObGeoType::GEOMETRY) 
                || (src_geo->type() >= ObGeoType::GEOTYPEMAX)) {
      ret = OB_ERR_INVALID_GEOMETRY_TYPE;
      LOG_WARN("unknown geometry type", K(ret), K(src_geo->type()));
    } else if (src_geo->type() <= ObGeoType::POLYGON) {
      // todo
    } else {
      uint32 idx = datum2->get_int();
      // check
      // get sub_geo
    }
  }

  return ret;
}


int ObExprSTGeometryN::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                              const ObRawExpr &raw_expr, 
                              ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_geometryn;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase