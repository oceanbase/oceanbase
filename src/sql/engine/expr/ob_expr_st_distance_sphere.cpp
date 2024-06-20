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
 * This file contains implementation for st_distance_sphere expr.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_distance_sphere.h"
#include "lib/geo/ob_geo_func_common.h"
#include "lib/geo/ob_geo_func_register.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprSTDistanceSphere::ObExprSTDistanceSphere(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_ST_DISTANCE_SPHERE,
                         N_ST_DISTANCE_SPHERE,
                         TWO_OR_THREE,
                         VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{
}

ObExprSTDistanceSphere::~ObExprSTDistanceSphere()
{

}

int ObExprSTDistanceSphere::calc_result_typeN(ObExprResType& type,
                                              ObExprResType* types_stack,
                                              int64_t param_num,
                                              common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_type(ObDoubleType);
  ObObjType type0 = types_stack[0].get_type();
  ObObjType type1 = types_stack[1].get_type();

  if (ob_is_null(type0) || ob_is_null(type1)) {
    // do nothing
  } else {
    if (!ob_is_geometry(type0)) {
      types_stack[0].set_calc_type(ObLongTextType);
      types_stack[0].set_calc_collation_type(CS_TYPE_BINARY);
      types_stack[0].set_calc_collation_level(CS_LEVEL_IMPLICIT);
    }
    if (!ob_is_geometry(type1)) {
      types_stack[1].set_calc_type(ObLongTextType);
      types_stack[1].set_calc_collation_type(CS_TYPE_BINARY);
      types_stack[1].set_calc_collation_level(CS_LEVEL_IMPLICIT);
    }
  }

  if (3 == param_num) {
    ObObjType radius_type = types_stack[2].get_type();
    if (ob_is_null(radius_type)) {
      // do nothing
    } else if (!ob_is_double_tc(radius_type)) {
      types_stack[2].set_calc_type(ObDoubleType);
    }
  }

  return ret;
}

int ObExprSTDistanceSphere::eval_st_distance_sphere(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    ObDatum &res)
{
  int ret = OB_SUCCESS;
  uint32_t arg_num = expr.arg_cnt_;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs1 = NULL;
  const ObSrsItem *srs2 = NULL;
  ObDatum *wkb1_datum = NULL;
  ObDatum *wkb2_datum = NULL;
  ObString wkb1;
  ObString wkb2;
  ObString wkb1_copy;
  ObString wkb2_copy;
  ObGeometry *g1 = NULL;
  ObGeometry *g2 = NULL;
  ObGeoSrid srid1 = UINT32_MAX;
  ObGeoSrid srid2 = UINT32_MAX;
  double sphere_radius = DEFAULT_SRID0_SPHERE_RADIUS;
  double result = 0.0;

  if (OB_FAIL(expr.args_[0]->eval(ctx, wkb1_datum))) {
    LOG_WARN("fail to eval wkb1 datum", K(ret));
  } else if (wkb1_datum->is_null()) {
    is_null_result = true;
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, wkb2_datum))) {
    LOG_WARN("fail to eval wkb2 datum", K(ret));
  } else if (wkb2_datum->is_null()) {
    is_null_result = true;
  } else if (FALSE_IT(wkb1 = wkb1_datum->get_string())) {
  } else if (FALSE_IT(wkb2 = wkb2_datum->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *wkb1_datum,
            expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb1))) {
    LOG_WARN("fail to get real string data", K(ret), K(wkb1));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *wkb2_datum,
            expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), wkb2))) {
    LOG_WARN("fail to get real string data", K(ret), K(wkb2));
  } else if (OB_FAIL(ob_write_string(tmp_allocator, wkb1, wkb1_copy))) {
    LOG_WARN("fail to copy wkb1", K(ret), K(wkb1));
  } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb1_copy, srs1,
      true, N_ST_DISTANCE_SPHERE))) {
    LOG_WARN("fail to get srs1 item", K(ret), K(wkb1_copy));
  } else if (OB_FAIL(ObGeoExprUtils::build_geometry(tmp_allocator, wkb1_copy,
      g1, srs1, N_ST_DISTANCE_SPHERE, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {
    LOG_WARN("fail to create geo1", K(ret), K(wkb1_copy));
  } else if (OB_FAIL(ob_write_string(tmp_allocator, wkb2, wkb2_copy))) {
    LOG_WARN("fail to copy wkb2", K(ret), K(wkb2));
  } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb2_copy, srs2,
      true, N_ST_DISTANCE_SPHERE))) {
    LOG_WARN("fail to get srs2 item", K(ret), K(wkb2_copy));
  } else if (OB_FAIL(ObGeoExprUtils::build_geometry(tmp_allocator, wkb2_copy,
      g2, srs2, N_ST_DISTANCE_SPHERE, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {
    LOG_WARN("fail to create geo2", K(ret), K(wkb2_copy));
  } else {
    srid1 = OB_ISNULL(srs1) ? 0:srs1->get_srid();
    srid2 = OB_ISNULL(srs2) ? 0:srs2->get_srid();
    if (srid1 != srid2) {
      ret = OB_ERR_GIS_DIFFERENT_SRIDS;
      LOG_USER_ERROR(OB_ERR_GIS_DIFFERENT_SRIDS, get_func_name(), srid1, srid2);
    } else if (OB_NOT_NULL(srs1)) { // Non-zero SRS overrides default radius.
      if (ObSrsType::PROJECTED_SRS == srs1->srs_type()) {
        char type_pair[ERR_INFO_LEN] = {0};
        snprintf(type_pair, ERR_INFO_LEN, "%s, %s", ObGeoTypeUtil::get_geo_name_by_type(g1->type()),
            ObGeoTypeUtil::get_geo_name_by_type(g2->type()));
        ret = OB_ERR_NOT_IMPLEMENTED_FOR_PROJECTED_SRS;
        LOG_USER_ERROR(OB_ERR_NOT_IMPLEMENTED_FOR_PROJECTED_SRS, get_func_name(), type_pair);
      } else {
        double a = srs1->semi_major_axis();
        double b = srs1->semi_minor_axis();
        if (a == b) {
          sphere_radius = 0; // Avoid possible loss of precission.
        } else {
          sphere_radius = ((2.0 * a + b) / 3.0); // Mean radius, as defined by the IUGG
        }
      }
    }
  }

  if (OB_SUCC(ret) && !is_null_result && arg_num == 3) {
    ObDatum *radius_datum = NULL;
    if (OB_FAIL(expr.args_[2]->eval(ctx, radius_datum))) {
      LOG_WARN("fail to eval radius datum", K(ret));
    } else if (radius_datum->is_null()) {
      is_null_result = true;
    } else {
      sphere_radius = radius_datum->get_double();
      if (sphere_radius <= 0.0) {
        ret = OB_ERR_NONPOSITIVE_RADIUS;
        LOG_USER_ERROR(OB_ERR_NONPOSITIVE_RADIUS, get_func_name(), sphere_radius);
      }
    }
  }

  if (OB_SUCC(ret) && !is_null_result) {
    ObGeoEvalCtx gis_context(&tmp_allocator, srs1);
    if (OB_FAIL(gis_context.append_val_arg(sphere_radius))) {
      LOG_WARN("fail to append sphere_radius to gis_context", K(ret), K(sphere_radius));
    } else if (OB_FAIL(gis_context.append_geo_arg(g1)) || OB_FAIL(gis_context.append_geo_arg(g2))) {
      LOG_WARN("fail to append geo arg to gis_context", K(ret));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::DistanceSphere>::gis_func::eval(gis_context, result))) {
      LOG_WARN("fail to eval distance sphere", K(ret));
      if (OB_ERR_LONGITUDE_OUT_OF_RANGE == ret) {
        LOG_USER_ERROR(OB_ERR_LONGITUDE_OUT_OF_RANGE, result, N_ST_DISTANCE_SPHERE, -180.0, 180.0);
      } else if (OB_ERR_LATITUDE_OUT_OF_RANGE == ret) {
        LOG_USER_ERROR(OB_ERR_LATITUDE_OUT_OF_RANGE, result, N_ST_DISTANCE_SPHERE, -90.0, 90.0);
      } else {
        ObGeoExprUtils::geo_func_error_handle(ret, N_ST_DISTANCE_SPHERE);
      }
    } else if (std::isinf(result)) {
      ret = OB_ERROR_OUT_OF_RANGE;
      LOG_WARN("INFINITY", K(ret), K(result));
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null_result) {
      res.set_null();
    } else {
      res.set_double(result);
    }
  }

  return ret;
}

int ObExprSTDistanceSphere::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                    const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_distance_sphere;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase