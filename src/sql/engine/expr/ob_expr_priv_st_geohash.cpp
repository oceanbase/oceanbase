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
 * This file contains implementation for _st_geohash.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_geohash.h"
#include "lib/geo/ob_geo_func_register.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTGeoHash::ObExprPrivSTGeoHash(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_GEOHASH, N_PRIV_ST_GEOHASH, MORE_THAN_ZERO,
        NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprPrivSTGeoHash::~ObExprPrivSTGeoHash()
{}

int ObExprPrivSTGeoHash::calc_result_typeN(
        ObExprResType &type,
        ObExprResType *types,
        int64_t param_num,
        ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 2)) {
    ObString fun_name(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, fun_name.length(), fun_name.ptr());
  } else {
    ObObjType type_geom = types[0].get_type();
    if (!ob_is_geometry(type_geom)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_GEOHASH);
      LOG_WARN("invalid geometry type", K(ret), K(type_geom));
    } else if (param_num == 2) {
      ObObjType prec_type = types[1].get_type();
      if ((ob_is_integer_type(prec_type) && ObTinyIntType != prec_type) || ob_is_null(prec_type)) {
        // do noting
      } else if (ob_is_string_type(prec_type)) {
        types[1].set_calc_type(ObIntType);
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("invalid precision type", K(ret), K(prec_type));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObCastMode cast_mode = type_ctx.get_cast_mode();
    cast_mode &= ~CM_WARN_ON_FAIL;      // make cast return error when fail
    type_ctx.set_cast_mode(cast_mode);  // cast mode only do work in new sql engine cast frame.
    type.set_varchar();
    type.set_collation_type(type_ctx.get_coll_type());
    type.set_collation_level(CS_LEVEL_IMPLICIT);
  }

  return ret;
}

int ObExprPrivSTGeoHash::calc_geohash(ObGeogBox *&gbox, int precision, ObStringBuffer &geohash_buf)
{
  int ret = OB_SUCCESS;
  const char base32[] = "0123456789bcdefghjkmnpqrstuvwxyz";
  char bits_mask[] = {16,8,4,2,1};
  int bit = 0;
  uint8 ch_index = 0;
  double lon = gbox->xmin + (gbox->xmax - gbox->xmin) / 2;
  double lat = gbox->ymin + (gbox->ymax - gbox->ymin) / 2;
  double lon_range[2], lat_range[2], mid;
  lon_range[0] = -180.0;
  lon_range[1] = 180.0;
  lat_range[0] = -90.0;
  lat_range[1] = 90.0;

  for (int i = 0; OB_SUCC(ret) && i < precision * 5; i++) {
    if (!(i & 1)) {
      // process longitude
      mid = (lon_range[0] + lon_range[1]) / 2;
      if (lon >= mid) {
        // for compatibility，not using "ObGeoBoxUtil::is_float_gteq" here
        ch_index |=  bits_mask[bit];
        lon_range[0] = mid;
      } else {
        lon_range[1] = mid;
      }
    } else {
      // process latitute
      mid = (lat_range[0] + lat_range[1]) / 2;
      if (lat >= mid) {
        // for compatibility，not using "ObGeoBoxUtil::is_float_gteq" here
        ch_index |=  bits_mask[bit];
        lat_range[0] = mid;
      } else {
        lat_range[1] = mid;
      }
    }
    if (bit < 4) {
      bit++;
    } else if (OB_FAIL(geohash_buf.append(&base32[ch_index], 1))) {
      LOG_WARN("char append failed", K(ret), K(ch_index));
    } else {
      bit = 0;
      ch_index = 0;
    }
  }
  return ret;
}

int ObExprPrivSTGeoHash::calc_precision(ObGeogBox *&gbox, ObGeogBox *&bounds, int &precision)
{
  int ret = OB_SUCCESS;
  bool finish_calc = false;
  double minx, miny, maxx, maxy;
  double latmax, latmin, lonmax, lonmin;
  double lonwidth, latwidth;
  double latmaxadjust, lonmaxadjust, latminadjust, lonminadjust;
  minx = gbox->xmin;
  maxx = gbox->xmax;
  miny = gbox->ymin;
  maxy = gbox->ymax;

  if (ObGeoBoxUtil::is_float_equal(minx, maxx) && ObGeoBoxUtil::is_float_equal(miny, maxy)) {
    // it's a point
    precision = 20;
  } else {
    lonmin = -180.0;
    lonmax = 180.0;
    latmin = -90.0;
    latmax = 90.0;
    int prec_calc = 0;
    while(!finish_calc) {
      lonwidth = lonmax - lonmin;
      latwidth = latmax - latmin;
      latmaxadjust = lonmaxadjust = latminadjust = lonminadjust = 0.0;
      if (ObGeoBoxUtil::is_float_gt(minx, lonmin + lonwidth / 2.0)) {
        lonminadjust = lonwidth / 2.0;
      } else if (ObGeoBoxUtil::is_float_lt(maxx, lonmax - lonwidth / 2.0)) {
        lonmaxadjust = -1 * lonwidth / 2.0;
      }
      if (lonminadjust || lonmaxadjust) {
        lonmin += lonminadjust;
        lonmax += lonmaxadjust;
        prec_calc++;
      } else {
        finish_calc = true;
      }
      if (!finish_calc) {
        if (ObGeoBoxUtil::is_float_gt(miny, latmin + latwidth / 2.0)) {
          latminadjust = latwidth / 2.0;
        } else if (ObGeoBoxUtil::is_float_lt(maxy, latmax - latwidth / 2.0)) {
          latmaxadjust = -1 * latwidth / 2.0;
        }
        if (latminadjust || latmaxadjust) {
          latmin += latminadjust;
          latmax += latmaxadjust;
          prec_calc++;
        } else {
          finish_calc = true;
        }
      }
    }
    // record bounds for future use
    bounds->xmin = lonmin;
    bounds->xmax = lonmax;
    bounds->ymin = latmin;
    bounds->ymax = latmax;
    precision = prec_calc / 5;
  }
  return ret;
}

int ObExprPrivSTGeoHash::get_gbox(lib::MemoryContext &mem_ctx, ObGeometry *&geo, ObGeogBox *&gbox)
{
  int ret = OB_SUCCESS;
  ObGeoEvalCtx box_ctx(mem_ctx);
  box_ctx.set_is_called_in_pg_expr(true);

  if (OB_FAIL(box_ctx.append_geo_arg(geo))) {
    LOG_WARN("build gis context failed", K(ret), K(box_ctx.get_geo_count()));
  } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(box_ctx, gbox))) {
    LOG_WARN("failed to do box functor failed", K(ret));
  } else if (ObGeoBoxUtil::is_float_gt(gbox->xmin, gbox->xmax)
              || ObGeoBoxUtil::is_float_gt(gbox->ymin , gbox->ymax)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_GEOHASH);
    LOG_WARN("Geometric bounds invalid", K(ret), K(gbox->xmin), K(gbox->xmax), K(gbox->ymin), K(gbox->ymax));
  } else if (ObGeoBoxUtil::is_float_lt(gbox->xmin, -180.0) || ObGeoBoxUtil::is_float_gt(gbox->xmax, 180.0)) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "longitude", N_PRIV_ST_GEOHASH);
    LOG_WARN("longitude is out of range", K(ret), K(gbox->xmin), K(gbox->xmax));
  } else if (ObGeoBoxUtil::is_float_lt(gbox->ymin, -90.0) || ObGeoBoxUtil::is_float_gt(gbox->ymax, 90.0)) {
    ret = OB_OPERATE_OVERFLOW;
    LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "latitude", N_PRIV_ST_GEOHASH);
    LOG_WARN("latitude is out of range", K(ret), K(gbox->xmin), K(gbox->xmax));
  }
  return ret;
}

int ObExprPrivSTGeoHash::process_input_geometry(
        const ObExpr &expr,
        ObEvalCtx &ctx,
        MultimodeAlloctor &allocator,
        bool &is_null_res,
        ObGeometry *&geo,
        int &precision)
{
  int ret = OB_SUCCESS;
  ObDatum *geo_datum = nullptr;
  ObExpr *geo_arg = expr.args_[0];
  ObGeoType geo_type;
  uint32_t geo_srid;
  bool is_geo_empty = true;

  if (OB_FAIL(geo_arg->eval(ctx, geo_datum))) {
    LOG_WARN("fail to eval args", K(ret));
  } else if (geo_datum->is_null()) {
    is_null_res = true;
  } else {
    ObString geo_wkb = geo_datum->get_string();
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *geo_srs = nullptr;

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                    allocator,
                    *geo_datum,
                    geo_arg->datum_meta_,
                    geo_arg->obj_meta_.has_lob_header(),
                    geo_wkb))) {
      LOG_WARN("fail to read real string data", K(ret), K(geo_arg->obj_meta_.has_lob_header()), K(geo_wkb));
    } else if (geo_wkb.empty()) {
      is_null_res = true;
    } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(geo_wkb, geo_type, geo_srid))) {
      if (ret == OB_ERR_GIS_INVALID_DATA) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_GEOHASH);
      }
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, geo_wkb, geo_srs, true, N_PRIV_ST_GEOHASH))) {
      LOG_WARN("fail to get srs item", K(ret), K(geo_wkb));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(
                          allocator,
                          geo_wkb,
                          geo,
                          nullptr,
                          N_PRIV_ST_GEOHASH,
                          ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {  // ObIWkbGeom
      LOG_WARN("fail to build geometry from wkb", K(ret), K(geo_wkb));
    } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo, is_geo_empty))) {
      LOG_WARN("check geo empty failed", K(ret));
    } else if (is_geo_empty) {
      is_null_res = true;
    }
  }

  if (OB_SUCC(ret) && !is_null_res && expr.arg_cnt_ == 2) {
    ObExpr *prec_arg = expr.args_[1];
    ObDatum *prec_datum = nullptr;
    if (ob_is_null(prec_arg->datum_meta_.type_)) {
      precision = 0;
    } else if (OB_FAIL(prec_arg->eval(ctx, prec_datum))) {
      LOG_WARN("fail to eval args", K(ret));
    } else if (prec_datum->is_null()) {
      precision = 0;
    } else {
      int64_t tmp_prec = prec_datum->get_int();
      if (tmp_prec > INT32_MAX || tmp_prec < INT32_MIN) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "precision", N_PRIV_ST_GEOHASH);
        LOG_WARN("precision is not in range", K(ret), K(tmp_prec));
      } else {
        precision = static_cast<int>(tmp_prec);
      }
    }
  }
  return ret;
}

int ObExprPrivSTGeoHash::eval_priv_st_geohash(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_null_res = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret, N_PRIV_ST_GEOHASH);
  ObGeometry *geo = nullptr;
  ObString geohash_res;
  ObGeogBox *gbox = nullptr;
  ObGeogBox *bounds = nullptr;
  ObStringBuffer geohash_buf(&temp_allocator);
  int precision = 0;

  if (OB_FAIL(process_input_geometry(expr, ctx, temp_allocator, is_null_res, geo, precision))) {
    LOG_WARN("fail to process input geometry", K(ret), K(is_null_res), K(geo), K(precision));
  }
  ObGeoBoostAllocGuard guard(tenant_id);
  lib::MemoryContext *mem_ctx = nullptr;
  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    res.set_null();
  } else if (OB_FAIL(guard.init())) {
    LOG_WARN("fail to init geo allocator guard", K(ret));
  } else if (OB_ISNULL(mem_ctx = guard.get_memory_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to get mem ctx", K(ret));
  } else if (OB_FAIL(get_gbox(*mem_ctx, geo, gbox))) {
    LOG_WARN("fail to calculate geometry gbox", K(ret));
  } else if (OB_ISNULL(bounds = OB_NEWx(ObGeogBox, &temp_allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), KP(bounds));
  } else if (precision <= 0 && OB_FAIL(calc_precision(gbox, bounds, precision))) {
    LOG_WARN("fail to calculate precision", K(ret));
  } else if (OB_FAIL(calc_geohash(gbox, precision, geohash_buf))) {
    LOG_WARN("fail to calculate geohash", K(ret));
  } else {
    ObExprStrResAlloc res_alloc(expr, ctx);
    char *res_buf = (char *)res_alloc.alloc(geohash_buf.length());
    if (OB_ISNULL(res_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("result buffer allocation failed", K(ret), K(geohash_buf.length()));
    } else {
      MEMCPY(res_buf, geohash_buf.ptr(), geohash_buf.length());
      res.set_string(res_buf, geohash_buf.length());
    }
  }

  return ret;
}

int ObExprPrivSTGeoHash::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_geohash;
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase