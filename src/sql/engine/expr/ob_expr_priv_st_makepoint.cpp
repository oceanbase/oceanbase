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
 * This file contains implementation for _st_makepoint.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_makepoint.h"
#include "sql/session/ob_sql_session_info.h"
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
ObExprPrivSTMakePoint::ObExprPrivSTMakePoint(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_MAKEPOINT, N_PRIV_ST_MAKEPOINT, TWO_OR_THREE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprPrivSTMakePoint::~ObExprPrivSTMakePoint()
{
}

int ObExprPrivSTMakePoint::calc_result_typeN(
        ObExprResType& type,
        ObExprResType* types_stack,
        int64_t param_num,
        ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  for (int i = 0; OB_SUCC(ret) && i < param_num; i++) {
    ObObjType type = types_stack[i].get_type();
    if (ob_is_null(type) || ob_is_double_type(type) || type == ObTinyIntType || ob_is_string_type(type)) {
      // do nothing
    } else if (ob_is_numeric_type(type)) {
      types_stack[i].set_calc_type(ObDoubleType);
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid input type", K(ret), K(i), K(type));
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

  return ret;
}

int ObExprPrivSTMakePoint::eval_priv_st_makepoint(
        const ObExpr &expr,
        ObEvalCtx &ctx,
        ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_null_result = false;
  int num_args = expr.arg_cnt_;
  ObDatum *datum[3] = {nullptr};
  double p[3] = {0.0};
  ObGeoSrid srid = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret, N_PRIV_ST_ASMVTGEOM);
  ObWkbBuffer res_wkb_buf(temp_allocator);

  for(int i = 0; OB_SUCC(ret) && i < num_args; i++) {
    if (expr.args_[i]->is_boolean_) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid type", K(ret), K(i), K(expr.args_[i]->is_boolean_));
    } else if(ob_is_null(expr.args_[i]->datum_meta_.type_)) {
      is_null_result = true;
    } else if (OB_FAIL(expr.args_[i]->eval(ctx, datum[i]))) {
      LOG_WARN("fail to eval arg", K(ret), K(expr.args_[i]->datum_meta_.type_));
    } else if (datum[i]->is_null()) {
      is_null_result = true;
    } else if (ob_is_string_type(expr.args_[i]->datum_meta_.type_)) {
      if (OB_FAIL(ObGeoExprUtils::string_to_double(datum[i]->get_string(), expr.args_[i]->datum_meta_.cs_type_, p[i]))) {
        LOG_WARN("fail to get arg", K(ret), K(expr.args_[i]->datum_meta_.type_));
      }
    } else {
      p[i] = datum[i]->get_double();
    }
  }

  if (OB_FAIL(ret) || is_null_result) {
  } else if (OB_FAIL(res_wkb_buf.append(srid))) {
    LOG_WARN("fail to append srid to makepoint wkb buf", K(ret), K(srid));
  } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ENCODE_GEO_VERSION(GEO_VESION_1))))) {
    LOG_WARN("fail to append version to makepoint wkb buf", K(ret));
  } else if (OB_FAIL(res_wkb_buf.append(static_cast<char>(ObGeoWkbByteOrder::LittleEndian)))) {
    LOG_WARN("fail to append little endian byte order to makepoint wkb buf", K(ret));
  } else if (num_args == 2 && OB_FAIL(res_wkb_buf.append(static_cast<uint32_t>(ObGeoType::POINT)))) {
    LOG_WARN("fail to append 2D point type to makepoint wkb buf", K(ret));
  } else if (num_args == 3 && OB_FAIL(res_wkb_buf.append(static_cast<uint32_t>(ObGeoType::POINTZ)))) {
    LOG_WARN("fail to append 3D point type to makepoint wkb buf", K(ret));
  } else if (OB_FAIL(res_wkb_buf.append(p[0]))) {
    LOG_WARN("fail to append x to makepoint wkb buf", K(ret), K(p[0]));
  } else if (OB_FAIL(res_wkb_buf.append(p[1]))) {
    LOG_WARN("fail to append y to makepoint wkb buf", K(ret), K(p[1]));
  } else if (num_args > 2 && OB_FAIL(res_wkb_buf.append(p[2]))) {
    LOG_WARN("fail to append z to makepoint wkb buf", K(ret), K(p[2]));
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

int ObExprPrivSTMakePoint::cg_expr(
        ObExprCGCtx &expr_cg_ctx,
        const ObRawExpr &raw_expr,
        ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_makepoint;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase