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
 * This file contains implementation for st_transform.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/geo/ob_geo_func_register.h"
#include "ob_expr_st_transform.h"
#include "lib/geo/ob_srs_info.h"
#include "observer/omt/ob_tenant_srs.h"
#include "lib/geo/ob_geo_normalize_visitor.h"
#include "lib/geo/ob_geo_wkb_size_visitor.h"
#include "lib/geo/ob_geo_wkb_visitor.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSTTransform::ObExprSTTransform(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_TRANSFORM, N_ST_TRANSFORM, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprSTTransform::calc_result_type2(ObExprResType &type,
                                         ObExprResType &type1,
                                         ObExprResType &type2,
                                         common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  ObObjType type_g = type1.get_type();
  ObObjType type_srid = type2.get_type();

  if (ob_is_null(type_g)) {
    // do nothing
  } else if (ob_is_numeric_type(type_g)) {
    type1.set_calc_type(ObLongTextType);
  } else if (!ob_is_geometry(type_g) && !ob_is_string_type(type_g)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_name());
  }

  if (OB_SUCC(ret)) {
    if (ob_is_null(type_srid)) {
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

int ObExprSTTransform::eval_st_transform(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum = NULL;
  ObDatum *datum2 = NULL;
  ObGeometry *src_geo = NULL;
  ObGeometry *dest_geo = NULL;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *src_srs_item = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObString src_proj4_param;
  ObString dest_proj4_param;
  bool need_eval = true;
  bool is_null_result = false;

  // check null
  is_null_result = ob_is_null(expr.args_[0]->datum_meta_.type_) || ob_is_null(expr.args_[1]->datum_meta_.type_);

  if (is_null_result) {
    res.set_null();
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, gis_datum))) {
    LOG_WARN("eval geo arg failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, datum2))) {
    LOG_WARN("eval sird arg failed", K(ret));
  } else if (gis_datum->is_null() || datum2->is_null()) {
    res.set_null();
  } else {
    uint32_t src_srid = 0;
    uint32_t dest_srid = 0;
    ObString wkb = gis_datum->get_string();
    const ObSrsItem *dest_srs_item = NULL;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum,
              expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, src_srs_item, true, N_ST_TRANSFORM))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(wkb, src_srid))) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_TRANSFORM);
      LOG_WARN("get srid from wkb failed", K(wkb), K(ret));
    } else {
      if (datum2->get_int() < 0 || datum2->get_int() > UINT_MAX32) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("srid input value out of range", K(ret), K(datum2->get_int()));
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "SRID", N_ST_TRANSFORM);
      }
      dest_srid = datum2->get_int();
      if (OB_SUCC(ret)) {
        if (dest_srid == src_srid) { // return src geo directly
          ObString res_wkb;
          if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb, src_geo, src_srs_item, N_ST_TRANSFORM, false, false, false))) {
            LOG_WARN("fail to create geo", K(ret), K(wkb));
          } else if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*src_geo, expr, ctx, src_srs_item, res_wkb))) {
            LOG_WARN("failed to write geometry to wkb", K(ret));
          } else {
            res.set_string(res_wkb);
            need_eval = false;
          }
        } else if (OB_ISNULL(src_srs_item)) { // valid only when dest srid is also 0
          ret = OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED, src_srid);
        } else if (dest_srid == 0) { // valid only when src srid is also 0
          ret = OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED, dest_srid);
        } else if (OB_FAIL(srs_guard.get_srs_item(dest_srid, dest_srs_item))) {
          LOG_WARN("failed to get dest srs", K(ret), K(dest_srid));
        } else if (OB_FAIL(dest_srs_item->get_proj4_param(&temp_allocator, dest_proj4_param))) {
          LOG_WARN("failed to get proj4 prams from dest srs", K(ret), K(dest_srid));
        }
      }
      if (OB_SUCC(ret) && need_eval) {
        if (src_srs_item->missing_towgs84()) {
          ret = OB_ERR_TRANSFORM_SOURCE_SRS_MISSING_TOWGS84;
          LOG_USER_ERROR(OB_ERR_TRANSFORM_SOURCE_SRS_MISSING_TOWGS84, src_srid);
          LOG_WARN("source srs is not WGS 84 and has no TOWGS84 clause", K(ret), K(src_srid));
        }
        if (OB_ISNULL(dest_srs_item)) {
          ret = OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED, dest_srid);
          LOG_WARN("dest srs is null", K(ret), K(dest_srid));
        } else if(dest_srs_item->missing_towgs84()) {
          ret = OB_ERR_TRANSFORM_TARGET_SRS_MISSING_TOWGS84;
          LOG_USER_ERROR(OB_ERR_TRANSFORM_TARGET_SRS_MISSING_TOWGS84, dest_srid);
          LOG_WARN("dest srs is not WGS 84 and has no TOWGS84 clause", K(ret), K(dest_srid));
        } else if (!src_srs_item->is_geographical_srs()) {
          ret = OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED, src_srid);
          LOG_WARN("src srs is not geog", K(ret), K(src_srid));
        } else if (!dest_srs_item->is_geographical_srs()) {
          ret = OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED, dest_srid);
          LOG_WARN("dest srs is not geog", K(ret), K(dest_srid));
        }
      }

      if (OB_SUCC(ret) && need_eval && src_srid != 0) {
        if (OB_FAIL(src_srs_item->get_proj4_param(&temp_allocator, src_proj4_param))) {
          LOG_WARN("failed to get proj4 prams from srs", K(ret), K(src_srid));
        }
      }

      // eval by bg
      if (OB_SUCC(ret) && need_eval) {
        int correct_result;
        ObGeoEvalCtx correct_context(&temp_allocator, src_srs_item);
        ObGeoEvalCtx transform_context(&temp_allocator, dest_srs_item);
        if (src_proj4_param.empty()) {
          ret = OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED, src_srid);
        } else if (dest_proj4_param.empty()) {
          ret = OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED, dest_srid);
        } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb, src_geo,
                                                          src_srs_item, N_ST_TRANSFORM))) {
          LOG_WARN("failed to parse wkb", K(ret));
        } else if (OB_FAIL(correct_context.append_geo_arg(src_geo))) {
          LOG_WARN("failed to append geo arg to gis context", K(ret), K(correct_context.get_geo_count()));
        } else if (OB_FAIL(transform_context.append_geo_arg(src_geo))) {
          LOG_WARN("failed to append geo arg to gis context", K(ret), K(transform_context.get_geo_count()));
        } else if (OB_FAIL(transform_context.append_val_arg(&src_proj4_param))) {
          LOG_WARN("failed to append src_proj4_param to gis context", K(ret), K(transform_context.get_geo_count()));
        } else if (OB_FAIL(transform_context.append_val_arg(&dest_proj4_param))) {
          LOG_WARN("failed to append dest_proj4_param to gis context", K(ret), K(transform_context.get_geo_count()));
        } else if (OB_NOT_NULL(src_srs_item) && OB_FAIL(ObGeoFunc<ObGeoFuncType::Correct>::geo_func::eval(correct_context, correct_result))) {
          LOG_WARN("eval boost correct failed", K(ret));
        } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Transform>::geo_func::eval(transform_context, dest_geo))) {
          LOG_WARN("eval boost transform failed", K(ret), K(src_proj4_param), K(dest_proj4_param));
          ObGeoExprUtils::geo_func_error_handle(ret, N_ST_TRANSFORM);
        } else if (dest_srs_item == NULL && OB_FAIL(ObGeoExprUtils::denormalize_wkb(dest_proj4_param, dest_geo))) {
          LOG_WARN("failed to do denormalize wkb", K(ret), K(dest_proj4_param));
        } else if ((OB_ISNULL(dest_srs_item) && dest_srid != 0) || OB_ISNULL(dest_geo)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected null srs_item or res_geo", K(ret), K(dest_srid), KP(dest_srs_item), KP(dest_geo));
        } else {
          ObString res_wkb;
          if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*dest_geo, expr, ctx, dest_srs_item, res_wkb, dest_srid))){
            LOG_WARN("failed to write geometry to wkb", K(ret));
          } else {
            res.set_string(res_wkb);
          }
        }
      }
    }
  }

  return ret;
}

int ObExprSTTransform::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_transform;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase