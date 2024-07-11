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
 * This file contains implementation for st_simplify.
 */
#define USING_LOG_PREFIX SQL_ENG
#include "lib/geo/ob_geo_func_register.h"
#include "ob_expr_st_simplify.h"
#include "lib/geo/ob_srs_info.h"
#include "observer/omt/ob_tenant_srs.h"
#include "lib/geo/ob_geo_wkb_visitor.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
// #include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSTSimplify::ObExprSTSimplify(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_SIMPLIFY, N_ST_SIMPLIFY, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprSTSimplify::calc_result_type2(ObExprResType &type,
                                         ObExprResType &type1,
                                         ObExprResType &type2,
                                         common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  ObObjType type_g = type1.get_type();
  ObObjType type_dist = type2.get_type();

  if (ob_is_null(type_g)) {
    // do nothing
  } else if (ob_is_numeric_type(type_g)) {
    type1.set_calc_type(ObLongTextType);
  } else if (!ob_is_geometry(type_g) && !ob_is_string_type(type_g)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_name());
  }

  if (OB_SUCC(ret)) {
    if (ob_is_null(type_dist)) {
      // do nothing
    } else {
      type2.set_calc_type(ObDoubleType);
    }
  }
  type.set_geometry();
  type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());

  return ret;
}

int ObExprSTSimplify::eval_st_simplify(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
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
    LOG_WARN("eval maxdistance arg failed", K(ret));
  } else if (gis_datum->is_null() || datum2->is_null()) {
    res.set_null();
  } else {
    uint32_t srid = 0;
    double max_distance = 0.0;
    ObString wkb = gis_datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum,
              expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs, true, N_ST_SIMPLIFY))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(wkb, srid))) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_SIMPLIFY);
      LOG_WARN("get srid from wkb failed", K(wkb), K(ret));
    } else {
      // check max_distance 
      if (datum2->get_double() < DOUBLE_TRUE_VALUE_THRESHOLD || 
          datum2->get_double() > DBL_MAX) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("max_distance input value out of range", K(ret), K(datum2->get_int()));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "st_simplify");
      } else
        max_distance = datum2->get_double();

      // eval by bg
      if (OB_SUCC(ret)) {
        ObGeoEvalCtx simplify_context(&temp_allocator, srs);
        if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb, src_geo,
                                                  srs, N_ST_SIMPLIFY, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {
          LOG_WARN("failed to parse wkb", K(ret));
        } else if (src_geo->crs() == ObGeoCRS::Geographic) {
          ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
          LOG_USER_ERROR(OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS, N_ST_SIMPLIFY, ObGeoTypeUtil::get_geo_name_by_type(src_geo->type()));
          LOG_WARN("src srs is not geog", K(ret), K(srid));
        } else if (OB_FAIL(simplify_context.append_geo_arg(src_geo))) {
          LOG_WARN("failed to append geo arg to gis context", K(ret), K(simplify_context.get_geo_count()));
        } else if (OB_FAIL(simplify_context.append_val_arg(max_distance))) {
          LOG_WARN("failed to append max_distance to gis context", K(ret), K(simplify_context.get_geo_count()));
        } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Simplify>::geo_func::eval(simplify_context, dest_geo))) {
          LOG_WARN("eval boost simplify failed", K(ret));
          ObGeoExprUtils::geo_func_error_handle(ret, N_ST_SIMPLIFY);
        } else {
          if (OB_ISNULL(dest_geo) || dest_geo->is_empty()) {
            res.set_null();
          } else {
            ObString res_wkb;
            if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*dest_geo, expr, ctx, srs, res_wkb, srid))){
              LOG_WARN("failed to write geometry to wkb", K(ret));
            } else {
              res.set_string(res_wkb);
            }
          }
        }
      }
    }
  }

  return ret;
}


int ObExprSTSimplify::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_simplify;
  return OB_SUCCESS;
}


} // namespace sql
} // namespace oceanbase