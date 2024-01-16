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
 * This file contains implementation for _st_makeenvelope.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_makeenvelope.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_utils.h"
#include "share/object/ob_obj_cast_util.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTMakeEnvelope::ObExprPrivSTMakeEnvelope(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_MAKEENVELOPE, N_PRIV_ST_MAKEENVELOPE, MORE_THAN_TWO,
        NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprPrivSTMakeEnvelope::~ObExprPrivSTMakeEnvelope()
{}

int ObExprPrivSTMakeEnvelope::calc_result_typeN(
    ObExprResType &type, ObExprResType *types_stack, int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  if (param_num != 4 && param_num != 5) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number, should be four or five", K(ret), K(param_num));
  } else {
    ObObjType cur_type = ObObjType::ObMaxType;
    for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
      cur_type = types_stack[i].get_type();
      if (!ob_is_numeric_type(cur_type) && !ob_is_string_type(cur_type) && !ob_is_null(cur_type)) {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("invalid input type x", K(ret), K(i), K(cur_type));
      } else if ((ob_is_numeric_type(cur_type) && !ob_is_double_type(cur_type)
                     && ObTinyIntType != cur_type)) {  // pass string type and boolean type
        types_stack[i].set_calc_type(ObDoubleType);
      }
    }

    if (OB_SUCC(ret) && param_num == 5) {
      cur_type = types_stack[4].get_type();
      if (!ob_is_integer_type(cur_type) && !ob_is_string_type(cur_type) && !ob_is_null(cur_type)) {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("invalid input type srid", K(ret), K(cur_type));
      } else if (ob_is_string_type(cur_type)) {
        types_stack[4].set_calc_type(ObIntType);
      }
    }

    if (OB_SUCC(ret)) {
      ObCastMode cast_mode = type_ctx.get_cast_mode();
      cast_mode &= ~CM_WARN_ON_FAIL;         // make cast return error when fail
      cast_mode |= CM_STRING_INTEGER_TRUNC;  // make cast check range when string to int
      type_ctx.set_cast_mode(cast_mode);     // cast mode only do work in new sql engine cast frame.
      type.set_geometry();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
    }
  }

  return ret;
}

// ob_obj_cast.cpp string_double does not return error
// when empty string is converted to double in mysql mode
int ObExprPrivSTMakeEnvelope::string_to_double(const common::ObString &in_str, ObCollationType cs_type, double &res)
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

int ObExprPrivSTMakeEnvelope::read_args(omt::ObSrsCacheGuard &srs_guard, const ObExpr &expr, ObEvalCtx &ctx, ObSEArray<double, 4> &coords,
    ObGeoSrid &srid, bool &is_null_result, const ObSrsItem *&srs_item)
{
  int ret = OB_SUCCESS;
  ObExpr *arg = nullptr;
  ObObjType type = ObObjType::ObMaxType;
  ObDatum *datum = nullptr;
  double coord = 0.0;

  // read args
  for (int i = 0; OB_SUCC(ret) && !is_null_result && i < 4; ++i) {
    arg = expr.args_[i];
    type = arg->datum_meta_.type_;
    if (arg->is_boolean_) {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("invalid type", K(ret), K(arg->is_boolean_));
    } else if (ob_is_null(type)) {
      is_null_result = true;
    } else if (OB_FAIL(arg->eval(ctx, datum))) {
      LOG_WARN("fail to eval arg", K(ret), K(i), K(type));
    } else if (datum->is_null()) {
      is_null_result = true;
    } else if (!ob_is_string_type(type)) {
      coord = type == ObTinyIntType ? datum->get_tinyint() : datum->get_double();
    } else if (OB_FAIL(string_to_double(datum->get_string(), arg->datum_meta_.cs_type_, coord))) {
      LOG_WARN("fail to get x", K(ret), K(datum->get_string()), K(arg->datum_meta_.cs_type_));
    }

    if (OB_SUCC(ret) && !is_null_result) {
      if (OB_FAIL(coords.push_back(coord))) {
        LOG_WARN("failed to read point params", K(ret), K(i), K(coord));
      }
    }
  }

  bool is_geog = false;
  if (expr.arg_cnt_ == 5 && !is_null_result && OB_SUCC(ret)) {
    if (expr.args_[4]->is_boolean_) {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("invalid type", K(ret));
    } else if (OB_FAIL(expr.args_[4]->eval(ctx, datum))) {
      LOG_WARN("fail to eval second argument", K(ret));
    } else if (datum->is_null()) {
      is_null_result = true;
    } else if (datum->get_int() < 0 || datum->get_int() > INT_MAX32) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "SRID", N_PRIV_ST_MAKEENVELOPE);
      LOG_WARN("srid input value out of range", K(ret), K(srid));
    } else if (0 != (srid = datum->get_uint32())) {
      if (OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
        LOG_WARN("fail to get srs guard", K(ret));
      } else if (OB_FAIL(srs_guard.get_srs_item(srid, srs_item))) {
        LOG_WARN("fail to get srs item", K(ret));
      } else if (OB_ISNULL(srs_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null srs item", K(ret));
      }
    }
  }
  return ret;
}

int ObExprPrivSTMakeEnvelope::eval_priv_st_makeenvelope(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObGeoSrid srid = 0;
  ObSEArray<double, 4> coords;  // rectangle point: xmin, ymin, xmax, ymax
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObWkbBuffer wkb_buf(tmp_allocator);
  ObWkbBuffer res_wkb_buf(tmp_allocator);
  ObGeometry *geo = NULL;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs_item = NULL;
  // rectangle point -> polygon ewkb
  if (OB_FAIL(read_args(srs_guard, expr, ctx, coords, srid, is_null_result, srs_item))) {
    LOG_WARN("fail to read args", K(ret));
  } else if (is_null_result) {
    res.set_null();
  } else {
    if (OB_FAIL(ObGeoTypeUtil::rectangle_to_swkb(coords[0], coords[1], coords[2], coords[3], srid, true, res_wkb_buf))) {
      LOG_WARN("fail to transform rectangle point to ewkb", K(ret), K(srid));
    } else if (OB_FAIL(ObGeoExprUtils::pack_geo_res(expr, ctx, res, res_wkb_buf.string()))) {
      LOG_WARN("fail to pack geo res", K(ret));
    }
  }

  return ret;
}

int ObExprPrivSTMakeEnvelope::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_makeenvelope;
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase