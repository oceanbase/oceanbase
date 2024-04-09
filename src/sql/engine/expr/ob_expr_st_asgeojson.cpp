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
 * This file contains implementation for _st_asgeojson.
 */
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_asgeojson.h"
#include "share/object/ob_obj_cast_util.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/json_type/ob_json_parse.h"
#include "observer/omt/ob_tenant_srs.h"
#include "lib/utility/ob_fast_convert.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "lib/geo/ob_wkb_to_json_visitor.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{
ObExprSTAsGeoJson::ObExprSTAsGeoJson(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_ASGEOJSON, N_ST_ASGEOJSON, MORE_THAN_ZERO,
          NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprSTAsGeoJson::~ObExprSTAsGeoJson()
{}

int ObExprSTAsGeoJson::calc_result_typeN(ObExprResType &type, ObExprResType *types_stack,
    int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (OB_UNLIKELY(param_num > 3)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    for (uint8_t i = 0; i < param_num && OB_SUCC(ret); i++) {
      ObObjType type = types_stack[i].get_type();
      if (i == 0) {
        // geometry
        if (!ob_is_geometry(type) && !ob_is_string_type(type) && !ob_is_null(type)) {
          ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
          LOG_WARN("invalid type for geometry", K(ret), K(type));
        }
      } else {
        if (!ob_is_integer_type(type) && !ob_is_null(type)
            && !ob_is_varchar_char_type(type, types_stack[i].get_collation_type())
            && !ob_is_enum_or_set_type(type)) {
          ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
          LOG_WARN("invalid type", K(ret), K(type));
        } else {
          types_stack[i].set_calc_type(ObIntType);
          type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
  }
  return ret;
}

int ObExprSTAsGeoJson::process_input_params(const ObExpr &expr, ObEvalCtx &ctx,
    ObIAllocator &allocator, ObGeometry *&geo, bool &is_null_res, ObGeoSrid& srid,
    uint32_t &max_dec_digits, uint8_t &flag)
{
  int ret = OB_SUCCESS;
  // geometry
  ObDatum *datum = nullptr;
  ObExpr *arg1 = expr.args_[0];
  ObObjType type1 = arg1->datum_meta_.type_;
  is_null_res = false;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = nullptr;
  if (ob_is_null(type1)) {
    is_null_res = true;
  } else if (OB_FAIL(arg1->eval(ctx, datum))) {
    LOG_WARN("fail to eval args", K(ret));
  } else if (datum->is_null()) {
    is_null_res = true;
  } else {
    // construct geometry
    ObString wkb = datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
            allocator, *datum, arg1->datum_meta_, arg1->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to read real string data", K(ret), K(arg1->obj_meta_.has_lob_header()));
    } else if (OB_FAIL(ObGeoExprUtils::construct_geometry(
                   allocator, wkb, srs_guard, srs, geo, N_ST_ASGEOJSON))) {
      LOG_WARN("fail to build geometry from wkb", K(ret), K(wkb));
    } else {
      srid = ObGeoWkbByteOrderUtil::read<uint32_t>(wkb.ptr(), ObGeoWkbByteOrder::LittleEndian);
    }
  }
  // max_dec_digits
  max_dec_digits = INT_MAX32;
  if (!is_null_res && OB_SUCC(ret) && expr.arg_cnt_ > 1) {
    if (OB_FAIL(expr.args_[1]->eval(ctx, datum))) {
      LOG_WARN("failed to eval second argument", K(ret));
    } else if (datum->is_null()) {
      is_null_res = true;
    } else if (datum->get_int() < 0 || datum->get_int() > INT_MAX32) {
      ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
      char flag_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
      int32_t len = ObFastFormatInt::format_signed(datum->get_int(), flag_buf);
      ObString string_type_str("max decimal digits");
      ObString func_str(N_ST_ASGEOJSON);
      LOG_USER_ERROR(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
          string_type_str.length(),
          string_type_str.ptr(),
          len,
          flag_buf,
          func_str.length(),
          func_str.ptr());
      LOG_WARN("Incorrect max decimal digits value for function st_asgeojson", K(ret), K(datum->get_int()));
    } else {
      max_dec_digits = datum->get_uint32();
    }
  }
  // flag
  flag = 0;
  if (!is_null_res && OB_SUCC(ret) && expr.arg_cnt_ > 2) {
    if (OB_FAIL(expr.args_[2]->eval(ctx, datum))) {
      LOG_WARN("failed to eval second argument", K(ret));
    } else if (datum->is_null()) {
      is_null_res = true;
    } else if (datum->get_int() < 0 || datum->get_int() > 7) {
      ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
      char flag_buf[ObFastFormatInt::MAX_DIGITS10_STR_SIZE] = {0};
      int32_t len = ObFastFormatInt::format_signed(datum->get_int(), flag_buf);
      ObString string_type_str("options");
      ObString func_str(N_ST_ASGEOJSON);
      LOG_USER_ERROR(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
          string_type_str.length(),
          string_type_str.ptr(),
          len,
          flag_buf,
          func_str.length(),
          func_str.ptr());
      LOG_WARN("Incorrect options value for function st_asgeojson", K(ret), K(datum->get_int()));
    } else {
      flag = datum->get_uint8();
    }
  }
  return ret;
}

int ObExprSTAsGeoJson::eval_st_asgeojson(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_null_res = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  uint32_t max_dec_digits = UINT_MAX32;
  uint8_t flag = 0;
  ObGeometry *geo = nullptr;
  ObString json_res;
  ObGeoSrid srid = 0;
  if (OB_FAIL(process_input_params(
          expr, ctx, temp_allocator, geo, is_null_res, srid, max_dec_digits, flag))) {
    LOG_WARN("fail to process input geometry", K(ret));
  } else if (!is_null_res) {
    // cal asgeojson
    ObWkbToJsonVisitor visitor(&temp_allocator, max_dec_digits, flag, srid);
    if (OB_FAIL(geo->do_visit(visitor))) {
      LOG_WARN("fail to do visit", K(ret));
    } else {
      visitor.get_geojson(json_res);
    }
  }
  uint32_t parse_flag = ObJsonParser::JSN_RELAXED_FLAG;
  ObJsonNode *j_tree = NULL;
  // set result
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_null_res) {
    res.set_null();
  } else if (OB_FAIL(ObJsonParser::get_tree(&temp_allocator, json_res, j_tree, parse_flag))) {
      LOG_WARN("fail to parse string as json", K(ret));
  } else {
    ObIJsonBase *j_base = j_tree;
    ObString raw_bin;
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
      LOG_WARN("fail to get string json binary", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_bin))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  }
  return ret;
}

int ObExprSTAsGeoJson::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_asgeojson;
  return OB_SUCCESS;
}
}  // namespace sql
}  // namespace oceanbase