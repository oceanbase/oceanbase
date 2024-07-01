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
 * This file contains implementation for _st_point.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_point.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/object/ob_obj_cast_util.h"
#include "lib/geo/ob_geo_func_common.h"
#include "lib/geo/ob_geo_common.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_bin.h"
#include "lib/geo/ob_geo.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTPoint::ObExprPrivSTPoint(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_POINT, N_PRIV_ST_POINT, TWO_OR_THREE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprPrivSTPoint::~ObExprPrivSTPoint()
{
}

int ObExprPrivSTPoint::calc_result_typeN(ObExprResType& type,
                                         ObExprResType* types_stack,
                                         int64_t param_num,
                                         ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  ObObjType type_x = types_stack[0].get_type();
  ObObjType type_y = types_stack[1].get_type();

  if (!ob_is_numeric_type(type_x)
      && !ob_is_string_type(type_x)
      && !ob_is_null(type_x)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid input type x", K(ret), K(type_x));
  } else if (!ob_is_numeric_type(type_y)
      && !ob_is_string_type(type_y)
      && !ob_is_null(type_y)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid input type y", K(ret), K(type_y));
  } else {
    if (ob_is_numeric_type(type_x)
        && !ob_is_double_type(type_x)
        && ObTinyIntType != type_x) { // pass string type and boolean type
      types_stack[0].set_calc_type(ObDoubleType);
    }
    if (ob_is_numeric_type(type_y)
        && !ob_is_double_type(type_y)
        && ObTinyIntType != type_y) { // pass string type and boolean type
      types_stack[1].set_calc_type(ObDoubleType);
    }
    if (param_num > 2) {
      ObObjType type_srid = types_stack[2].get_type();
      const ObSQLSessionInfo *session =
      dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cast basic session to sql session info failed", K(ret));
      } else if (!ob_is_integer_type(type_srid)
          && !ob_is_string_type(type_srid)
          && !ob_is_null(type_srid)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("invalid input type srid", K(ret), K(type_srid));
      } else if (ob_is_string_type(type_srid)) {
        types_stack[2].set_calc_type(ObIntType);
      }
    }

    if (OB_SUCC(ret)) {
      ObCastMode cast_mode = type_ctx.get_cast_mode();
      cast_mode &= ~CM_WARN_ON_FAIL; // make cast return error when fail
      cast_mode |= CM_STRING_INTEGER_TRUNC; // make cast check range when string to int
      type_ctx.set_cast_mode(cast_mode); // cast mode only do work in new sql engine cast frame.
      type.set_geometry();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
    }
  }

  return ret;
}

int ObExprPrivSTPoint::string_to_double(const common::ObString &in_str, ObCollationType cs_type,
                                        double &res)
{
  int ret = OB_SUCCESS;

  if (in_str.empty()) {
    ret = OB_ERR_DOUBLE_TRUNCATED;
    LOG_WARN("input string is empty", K(ret), K(in_str));
  } else {
    int err = 0;
    char *endptr = NULL;
    double out_val = ObCharset::strntodv2(in_str.ptr(), in_str.length(), &endptr, &err);
    if (EOVERFLOW == err && (-DBL_MAX == out_val || DBL_MAX == out_val)) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("value is out of range", K(ret), K(out_val));
    } else {
      if (OB_FAIL(check_convert_str_err(in_str.ptr(), endptr, in_str.length(), err, cs_type))) {
        LOG_WARN("fail to check convert str err", K(ret), K(in_str), K(out_val), K(err));
        ret = OB_ERR_DOUBLE_TRUNCATED;
      } else {
        res = out_val;
      }
    }
  }

  return ret;
}

int ObExprPrivSTPoint::eval_priv_st_point(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObDatum &res)
{
	int ret = OB_SUCCESS;
  bool is_null_result = false;
  int num_args = expr.arg_cnt_;
  ObDatum *datum_x = nullptr;
  ObDatum *datum_y = nullptr;
  ObDatum *datum_srid = nullptr;
  ObExpr *arg_x = expr.args_[0];
  ObExpr *arg_y = expr.args_[1];
  ObObjType type_x = arg_x->datum_meta_.type_;
  ObObjType type_y = arg_y->datum_meta_.type_;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObWkbBuffer res_wkb_buf(tmp_allocator);
  ObGeoSrid srid = 0;
  const ObSrsItem *srs_item = NULL;
  omt::ObSrsCacheGuard srs_guard;
  bool is_geog = false;

  if (arg_x->is_boolean_ || arg_y->is_boolean_) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid type", K(ret), K(arg_x->is_boolean_), K(arg_y->is_boolean_));
  } else if (ob_is_null(type_x) || ob_is_null(type_y)) {
    is_null_result = true;
  } else if (OB_FAIL(arg_x->eval(ctx, datum_x))) {
    LOG_WARN("fail to eval point x arg", K(ret), K(type_x));
  } else if (OB_FAIL(arg_y->eval(ctx, datum_y))) {
    LOG_WARN("fail to eval point y arg", K(ret), K(type_y));
  } else if (datum_x->is_null() || datum_y->is_null()) {
    is_null_result = true;
  }

  // get srid
  if (!is_null_result && OB_SUCC(ret) && num_args > 2) {
    if (expr.args_[2]->is_boolean_) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid type", K(ret));
    } else if (OB_FAIL(expr.args_[2]->eval(ctx, datum_srid))) {
      LOG_WARN("fail to eval second argument", K(ret));
    } else if (datum_srid->is_null()) {
      is_null_result = true;
    } else if (datum_srid->get_int() < 0 || datum_srid->get_int() > UINT_MAX32) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "SRID", N_PRIV_ST_POINT);
      LOG_WARN("srid input value out of range", K(ret), K(datum_srid->get_int()));
    } else if (0 != (srid = datum_srid->get_uint32())) {
      if (OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
        LOG_WARN("fail to get srs guard", K(ret));
      } else if (OB_FAIL(srs_guard.get_srs_item(srid, srs_item))) {
        LOG_WARN("fail to get srs item", K(ret));
      } else if (OB_ISNULL(srs_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null srs item", K(ret));
      } else {
        is_geog = srs_item->is_geographical_srs();
      }
    }
  }

  if (OB_SUCC(ret) && !is_null_result) {
    double x = 0.0;
    double y = 0.0;
    if (!ob_is_string_type(type_x)) {
      x = datum_x->get_double();
    }
    if (!ob_is_string_type(type_y)) {
      y = datum_y->get_double();
    }
    if (ob_is_string_type(type_x) && OB_FAIL(string_to_double(datum_x->get_string(), arg_x->datum_meta_.cs_type_, x))) {
      LOG_WARN("fail to get x", K(ret), K(type_x));
    } else if (ob_is_string_type(type_y) && OB_FAIL(string_to_double(datum_y->get_string(), arg_y->datum_meta_.cs_type_, y))) {
      LOG_WARN("fail to get y", K(ret), K(type_y));
    } else if (OB_FAIL(res_wkb_buf.append(srid))) {
      LOG_WARN("fail to append srid to point wkb buf", K(ret), K(srid));
    } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ENCODE_GEO_VERSION(GEO_VESION_1))))) {
      LOG_WARN("fail to append version to point wkb buf", K(ret));
    } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
      LOG_WARN("fail to append little endian byte order to point wkb buf", K(ret));
    } else if (OB_FAIL(res_wkb_buf.append(static_cast<uint32_t>(ObGeoType::POINT)))) {
      LOG_WARN("fail to append geo type to point wkb buf", K(ret));
    } else if (OB_FAIL(res_wkb_buf.append(x))) {
      LOG_WARN("fail to append x to point wkb buf", K(ret), K(x));
    } else if (OB_FAIL(res_wkb_buf.append(y))) {
      LOG_WARN("fail to append y to point wkb buf", K(ret), K(y));
    }

    if (OB_SUCC(ret) && is_geog) {
      double out_of_range_val;
      double longti = x;
      longti *= srs_item->angular_unit();
      if (longti <= -M_PI || longti > M_PI) {
        double min_long_val = 0.0;
        double max_long_val = 0.0;
        if (OB_FAIL(srs_item->from_radians_to_srs_unit(longti, out_of_range_val))) {
          LOG_WARN("fail to convert radians to srs unit", K(ret), K(longti), K(srs_item));
        } else if (OB_FAIL(srs_item->longtitude_convert_from_radians(-M_PI, min_long_val))) {
          LOG_WARN("fail to convert longitude from radians", K(ret));
        } else if (OB_FAIL(srs_item->longtitude_convert_from_radians(M_PI, max_long_val))) {
          LOG_WARN("fail to convert longitude from radians", K(ret));
        } else {
          ret = OB_ERR_LONGITUDE_OUT_OF_RANGE;
          LOG_USER_ERROR(OB_ERR_LONGITUDE_OUT_OF_RANGE, out_of_range_val, N_PRIV_ST_POINT, min_long_val, max_long_val);
          LOG_WARN("geometry longitude is out of range", "longitude value", out_of_range_val);
        }
      } else {
        double lati = y;
        lati *= srs_item->angular_unit();
        if (lati <= -M_PI_2 || lati > M_PI_2) {
          double min_lat_val = 0.0;
          double max_lat_val = 0.0;
          if (OB_FAIL(srs_item->from_radians_to_srs_unit(lati, out_of_range_val))) {
            LOG_WARN("fail to convert radians to srs unit", K(ret), K(lati), K(srs_item));
          } else if (OB_FAIL(srs_item->latitude_convert_from_radians(-M_PI_2, min_lat_val))) {
            LOG_WARN("fail to convert latitude from radians", K(ret));
          } else if (OB_FAIL(srs_item->latitude_convert_from_radians(M_PI_2, max_lat_val))) {
            LOG_WARN("fail to convert latitude from radians", K(ret));
          } else {
            ret = OB_ERR_LATITUDE_OUT_OF_RANGE;
            LOG_USER_ERROR(OB_ERR_LATITUDE_OUT_OF_RANGE, out_of_range_val, N_PRIV_ST_POINT, min_lat_val, max_lat_val);
            LOG_WARN("geometry latitude is out of range", "latitude value", out_of_range_val);
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null_result) {
      res.set_null();
    } else if (OB_FAIL(ObGeoExprUtils::pack_geo_res(expr, ctx, res, res_wkb_buf.string()))) {
      LOG_WARN("fail to pack geo res", K(ret));
    }
  }

  return ret;
}

int ObExprPrivSTPoint::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_point;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase