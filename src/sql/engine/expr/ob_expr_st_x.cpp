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
 * This file contains implementation for st_x, st_y, st_latitude, st_longtitude.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/geo/ob_geo_func_common.h"
#include "sql/engine/expr/ob_expr_st_x.h"
#include "lib/geo/ob_geo.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_srs_info.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

int ObExprSTCoordinate::calc_result_typeN(ObExprResType& type,
                                          ObExprResType* types_stack,
                                          int64_t param_num,
                                          ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  ObObjType geo_type = types_stack[0].get_type();
  if (ob_is_null(geo_type)) {
    // do nothing
  } else if (ob_is_numeric_type(geo_type)) {
    types_stack[0].set_calc_type(ObLongTextType);
  } else if (!ob_is_geometry(geo_type) && !ob_is_string_type(geo_type)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_name());
  }
  if (OB_SUCC(ret) && param_num == 2) {
    if (ob_is_null(types_stack[1].get_type()) || ob_is_double_type(types_stack[1].get_type())) {
      // do nothing
    } else {
      types_stack[1].set_calc_type(ObDoubleType);
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_WARN_ON_FAIL);
    }
  }

  if (OB_SUCC(ret)) {
    if (param_num == 1) {
      type.set_double();
    } else if (param_num == 2) {
      type.set_type(ObGeometryType);
      type.set_collation_level(common::CS_LEVEL_COERCIBLE);
      type.set_collation_type(CS_TYPE_BINARY);
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
    }
  }
  return ret;
}

int ObExprSTCoordinate::eval_common(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    ObDatum &res,
                                    bool is_first_d,
                                    bool only_geog,
                                    const char *func_name)
{
  int ret = OB_SUCCESS;
  int num_args = expr.arg_cnt_;
  ObDatum *datum = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObIWkbPoint *point = NULL;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = NULL;
  uint32_t srid = 0;
  ObGeometry *geo = NULL;
  bool is_geog = false;
  bool is_lat_long = false;
  bool is_null_result = false;
  bool is_long_res = is_first_d; // get or change long value ?
  ObObjType datum_type = ObMaxType;

  // get wkb
  if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("failed to eval first argument", K(ret));
  } else if (datum->is_null()) {
    is_null_result = true;
  } else {
    ObString wkb = datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *datum,
              expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs, true, func_name))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb,
        geo, srs, func_name, false))) {
      LOG_WARN("failed to create geometry object with raw wkb", K(ret));
    } else if (ObGeoType::POINT != geo->type()) {
      ret = OB_ERR_UNEXPECTED_GEOMETRY_TYPE;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED_GEOMETRY_TYPE, "POINT",
                      ObGeoTypeUtil::get_geo_name_by_type(geo->type()), func_name);
      LOG_WARN("unexpect geometry type, should be point", K(ret), "geo_type", geo->type());
    } else if (FALSE_IT(point = dynamic_cast<ObIWkbPoint *>(geo))) {
    } else if (OB_ISNULL(point)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ObIWkbPoint pointer", K(ret));
    } else if ((srid = geo->get_srid()) != 0) {
      is_geog = srs->is_geographical_srs();
      is_lat_long = srs->is_lat_long_order();
    }
  }
  if (OB_SUCC(ret) && !is_null_result) {
    if (only_geog && !is_geog) {
      ret = OB_ERR_SRS_NOT_GEOGRAPHIC;
      LOG_USER_ERROR(OB_ERR_SRS_NOT_GEOGRAPHIC, func_name, srid);
      LOG_WARN("only geographical srs is allowed", K(ret), K(srid));
    } else if (!only_geog){
      is_long_res = is_first_d ^ is_lat_long;
    }
  }
  // set result
  if (OB_FAIL(ret)) {
  } else if (is_null_result) {
    res.set_null();
  } else if (num_args == 1) {
    double d = is_long_res ? point->x() : point->y();
    res.set_double(d);
  } else if (num_args == 2) {
    if (OB_FAIL(expr.args_[1]->eval(ctx, datum))) {
      LOG_WARN("failed to eval", K(ret));
    } else if (datum->is_null()) {
      res.set_null();
    } else {
      double new_val = datum->get_double();
      if (is_geog) {
        double new_val_radian = 0.0;
        if (OB_FAIL(srs->from_srs_unit_to_radians(new_val, new_val_radian))) {
          LOG_WARN("failed to convert longitude to radians");
        } else if (is_long_res) {
          // check longitude
          if (OB_FAIL(check_longitude(new_val_radian, srs, new_val, func_name))) {
            LOG_WARN("failed to check longitude", K(ret));
          }
        } else {
          // check latitude
          if (OB_FAIL(check_latitude(new_val_radian, srs, new_val, func_name))) {
            LOG_WARN("failed to check latitude", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(point)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null point or srs item", K(ret), KP(point));
        } else {
          is_long_res ? point->x(new_val) : point->y(new_val);
          ObString res_wkb;
          if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*point, expr, ctx, srs, res_wkb))) {
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

int ObExprSTCoordinate::check_longitude(double new_val_radian,
                                        const ObSrsItem *srs,
                                        double new_val,
                                        const char *func_name)
{
  int ret = OB_SUCCESS;
  double max_long_val = 0.0;
  double min_long_val = 0.0;

  if (new_val_radian <= -M_PI || new_val_radian > M_PI) {
    if (OB_FAIL(srs->longtitude_convert_from_radians(-M_PI, min_long_val))) {
      LOG_WARN("failed to convert longitude from radians", K(ret));
    } else if (OB_FAIL(srs->longtitude_convert_from_radians(M_PI, max_long_val))) {
      LOG_WARN("failed to convert longitude from radians", K(ret));
    } else {
      ret = OB_ERR_LONGITUDE_OUT_OF_RANGE;
      LOG_USER_ERROR(OB_ERR_LONGITUDE_OUT_OF_RANGE, new_val, func_name, min_long_val, max_long_val);
      LOG_WARN("longitude value is out of range", K(ret), K(new_val), K(new_val_radian));
    }
  }

  return ret;
}

int ObExprSTCoordinate::check_latitude(double new_val_radian,
                                       const ObSrsItem *srs,
                                       double new_val,
                                       const char *func_name)
{
  int ret = OB_SUCCESS;
  double max_lat_val = 0.0;
  double min_lat_val = 0.0;

  if (new_val_radian < -M_PI_2 || new_val_radian > M_PI_2) {
    if (OB_FAIL(srs->latitude_convert_from_radians(-M_PI_2, min_lat_val))) {
      LOG_WARN("failed to convert latitude from radians", K(ret));
    } else if (OB_FAIL(srs->latitude_convert_from_radians(M_PI_2, max_lat_val))) {
      LOG_WARN("failed to convert latitude from radians", K(ret));
    } else {
      ret = OB_ERR_LATITUDE_OUT_OF_RANGE;
      LOG_USER_ERROR(OB_ERR_LATITUDE_OUT_OF_RANGE, new_val, func_name, min_lat_val, max_lat_val);
      LOG_WARN("latitude value is out of range", K(ret), K(new_val), K(new_val_radian));
    }
  }

  return ret;
}

int ObExprSTX::eval_st_x(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_common(expr, ctx, res, true, false, N_ST_X);
}

int ObExprSTX::cg_expr(ObExprCGCtx &expr_cg_ctx,
                       const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_x;
  return OB_SUCCESS;
}

int ObExprSTY::eval_st_y(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_common(expr, ctx, res, false, false, N_ST_Y);
}

int ObExprSTY::cg_expr(ObExprCGCtx &expr_cg_ctx,
                       const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_y;
  return OB_SUCCESS;
}

int ObExprSTLatitude::eval_st_latitude(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_common(expr, ctx, res, false, true, N_ST_LATITUDE);
}

int ObExprSTLatitude::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_latitude;
  return OB_SUCCESS;
}

int ObExprSTLongitude::eval_st_longitude(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_common(expr, ctx, res, true, true, N_ST_LONGITUDE);
}

int ObExprSTLongitude::cg_expr(ObExprCGCtx &expr_cg_ctx,
                               const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_longitude;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase