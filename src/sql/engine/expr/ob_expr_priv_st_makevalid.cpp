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
 * This file contains implementation for _st_makevalid.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_makevalid.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_to_tree_visitor.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTMakeValid::ObExprPrivSTMakeValid(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_MAKE_VALID, N_PRIV_ST_MAKEVALID, ZERO_OR_ONE,
        VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprPrivSTMakeValid::~ObExprPrivSTMakeValid()
{}

int ObExprPrivSTMakeValid::calc_result_typeN(
    ObExprResType &type, ObExprResType *types_stack, int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  if (param_num != 1) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number, should be four or five", K(ret), K(param_num));
  } else if (!ob_is_geometry(types_stack[0].get_type())
             && !ob_is_string_type(types_stack[0].get_type())
             && types_stack[0].get_type() != ObNullType) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_WARN("invalid type", K(ret), K(types_stack[0].get_type()));
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_MAKEVALID);
  } else {
    ObCastMode cast_mode = type_ctx.get_cast_mode();
    cast_mode &= ~CM_WARN_ON_FAIL; // make cast return error when fail
    type.set_geometry();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  }

  return ret;
}

int ObExprPrivSTMakeValid::eval_priv_st_makevalid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObDatum *gis_datum1 = nullptr;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = nullptr;
  ObGeometry *geo = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, gis_datum1))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (gis_datum1->is_null()) {
    res.set_null();
  } else if (OB_FAIL(ObGeoExprUtils::get_input_geometry(tmp_allocator, gis_datum1, ctx,
                                                        expr.args_[0], srs_guard, N_PRIV_ST_MAKEVALID, srs, geo))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (geo->crs() != ObGeoCRS::Cartesian) {
    ret = OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS;
    LOG_USER_ERROR(OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS, N_PRIV_ST_MAKEVALID,
                  ObGeoTypeUtil::get_geo_name_by_type(geo->type()));
  } else {
    bool isvalid_res = false;
    int correct_result;
    ObGeoNormalVal reason;
    ObGeoEvalCtx gis_context(&tmp_allocator, srs);
    if (OB_FAIL(gis_context.append_geo_arg(geo))) {
      LOG_WARN("build geo gis context failed", K(ret));
    } else if (OB_FAIL(gis_context.append_val_arg(reason))) {
      LOG_WARN("add reason val to context failed", K(ret));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::IsValid>::geo_func::eval(gis_context, isvalid_res))) {
      LOG_WARN("eval geo func isvalid failed", K(ret));
    } else if (!isvalid_res) {
      if (geo->type() == ObGeoType::POLYGON || geo->type() == ObGeoType::MULTIPOLYGON) {
        ObGeoToTreeVisitor to_tree(&tmp_allocator);
        ObGeometry *valid_geo = nullptr;
        if (OB_FAIL(geo->do_visit(to_tree))) {
          LOG_WARN("fail to transfer geo1 to tree", K(ret));
        } else {
          if (OB_FAIL(ObGeoExprUtils::make_valid_polygon(to_tree.get_geometry(), tmp_allocator, valid_geo))) {
            LOG_WARN("make polygon valid failed", K(ret));
          } else if (OB_NOT_NULL(valid_geo) && !valid_geo->is_empty()) {
            geo = valid_geo;
          }
        }
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Correct>::geo_func::eval(gis_context, correct_result))) {
        LOG_WARN("eval boost correct failed", K(ret));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::IsValid>::geo_func::eval(gis_context, isvalid_res))) {
        LOG_WARN("eval geo func isvalid failed", K(ret));
      } else if (!isvalid_res) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid result geo", K(ret), K(reason.int64_));
      }
    }
    if (OB_SUCC(ret)) {
      ObString res_wkb;
      if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*geo, expr, ctx, srs, res_wkb))){
        LOG_WARN("failed to write geometry to wkb", K(ret));
      } else {
        res.set_string(res_wkb);
      }
    }
  }

  return ret;
}

int ObExprPrivSTMakeValid::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_makevalid;
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase