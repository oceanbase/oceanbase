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
 * This file contains implementation for st_astext.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_asewkt.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_reverse_coordinate_visitor.h"
#include "lib/geo/ob_geo_to_wkt_visitor.h"
#include "lib/geo/ob_geo_func_common.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTAsEwkt::ObExprPrivSTAsEwkt(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_ASEWKT, N_PRIV_ST_ASEWKT, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprPrivSTAsEwkt::~ObExprPrivSTAsEwkt()
{
}

int ObExprPrivSTAsEwkt::calc_result_typeN(ObExprResType& type,
                                          ObExprResType* types_stack,
                                          int64_t param_num,
                                          ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 2)) {
    ObString fun_name(N_PRIV_ST_ASEWKT);
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, fun_name.length(), fun_name.ptr());
  } else {
    if (ob_is_geometry(types_stack[0].get_type())
        || ob_is_null(types_stack[0].get_type())) {
      // do nothing
    } else if (ob_is_string_type(types_stack[0].get_type())) {
      types_stack[0].set_calc_type(ObGeometryType);
      types_stack[0].set_calc_collation_type(CS_TYPE_BINARY);
      types_stack[0].set_calc_collation_level(CS_LEVEL_IMPLICIT);
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("invalid type", K(ret), K(types_stack[0].get_type()));
    }

    if (OB_SUCC(ret) && param_num > 1) {
      if (ob_is_integer_type(types_stack[1].get_type())
          || ob_is_null(types_stack[1].get_type())) {
        // do nothing
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("invalid type", K(ret), K(types_stack[1].get_type()));
      }
    }
    if (OB_SUCC(ret)) {
      type.set_type(ObLongTextType);
      type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
    }
  }
  return ret;
}

/*
 * Input is either ob geometry or swkb.
 * axis-order issue
 * may still need srs for validation input swkb in cast(hexstring->obgeometry)
 */
int ObExprPrivSTAsEwkt::eval_priv_st_asewkt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  int num_args = expr.arg_cnt_;
  bool is_null_result = false;
  ObString res_wkt;
  ObDatum *gis_datum = NULL;
  int64_t maxdecimaldigits = DEFAULT_DIGITS_IN_DOUBLE;

  // get geo
  if (OB_FAIL(expr.args_[0]->eval(ctx, gis_datum))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (gis_datum->is_null()) {
    is_null_result = true;
  } else if (num_args > 1) { // get maxdecimaldigits
    ObDatum *precsion_data = NULL;
    if (OB_FAIL(expr.args_[1]->eval(ctx, precsion_data))) {
      LOG_WARN("eval maxdecimaldigits args failed", K(ret));
    } else if (precsion_data->is_null()){
      is_null_result = true;
    } else {
      maxdecimaldigits = precsion_data->get_int();
    }
  } else { /* do nothing */ }

  if (OB_SUCC(ret)) {
    ObString wkb = gis_datum->get_string();
    if (is_null_result) {
      res.set_null();
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *gis_datum,
               expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoTypeUtil::geo_to_ewkt(wkb,
                                                  res_wkt,
                                                  tmp_allocator,
                                                  maxdecimaldigits))) {
      LOG_WARN("eval geo to ewkt failed", K(ret), K(wkb), K(maxdecimaldigits));
    } else if (OB_FAIL(ObGeoExprUtils::pack_geo_res(expr, ctx, res, res_wkt))) {
      LOG_WARN("fail to pack geo res", K(ret));
    }
  }

  return ret;
}

int ObExprPrivSTAsEwkt::calc_resultN(common::ObObj &result,
                                     const common::ObObj *objs,
                                     int64_t param_num,
                                     common::ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  bool is_null_result = false;
  ObString res_wkt;
  int64_t maxdecimaldigits = DEFAULT_DIGITS_IN_DOUBLE;

  // get geo
  if (objs[0].is_null()) {
    is_null_result = true;
  } else if (param_num > 1) { // get maxdecimaldigits
    if (objs[1].is_null()){
      is_null_result = true;
    } else {
      maxdecimaldigits = objs[1].get_int();
    }
  } else { /* do nothing */ }

  if (OB_SUCC(ret)) {
    ObString wkb = objs[0].get_string();
    if (is_null_result) {
      result.set_null();
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator, objs[0], wkb))) {
      LOG_WARN("fail to get real data", K(ret), K(objs[0]), K(wkb));
    } else if (OB_FAIL(ObGeoTypeUtil::geo_to_ewkt(wkb,
                                                  res_wkt,
                                                  tmp_allocator,
                                                  maxdecimaldigits))) {
      LOG_WARN("eval geo to ewkt failed", K(ret), K(wkb), K(maxdecimaldigits));
    } else {
      ObTextStringObObjResult text_result(ObGeometryType, nullptr, &result, true);
      if (OB_FAIL(text_result.init(res_wkt.length(), expr_ctx.calc_buf_))) {
        LOG_WARN("init lob result failed");
      } else if (OB_FAIL(text_result.append(res_wkt.ptr(), res_wkt.length()))) {
        LOG_WARN("failed to append realdata", K(ret), K(res_wkt), K(text_result));
      } else {
        text_result.set_result();
      }
    }
  }

  return ret;
}

int ObExprPrivSTAsEwkt::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_asewkt;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase