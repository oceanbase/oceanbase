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
 * This file contains implementation for eval_st_disjoint.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "ob_expr_st_disjoint.h"
#include "lib/geo/ob_geo_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSTDisjoint::ObExprSTDisjoint(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_DISJOINT, N_ST_DISJOINT, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTDisjoint::~ObExprSTDisjoint()
{
}

int ObExprSTDisjoint::calc_result_type2(ObExprResType &type,
                                        ObExprResType &type1,
                                        ObExprResType &type2,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (type1.get_type() == ObNullType) {
  } else if (!ob_is_geometry(type1.get_type()) && !ob_is_string_type(type1.get_type())) {
    type1.set_calc_type(ObVarcharType);
    type1.set_calc_collation_type(CS_TYPE_BINARY);
  }
  if (type2.get_type() == ObNullType) {
  } else if (!ob_is_geometry(type2.get_type()) && !ob_is_string_type(type2.get_type())) {
    type2.set_calc_type(ObVarcharType);
    type2.set_calc_collation_type(CS_TYPE_BINARY);
  }
  type.set_int32();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  return ret;
}

int ObExprSTDisjoint::eval_st_disjoint(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  bool is_geo1_empty = false;
  bool is_geo2_empty = false;
  ObGeometry *geo1 = NULL;
  ObGeometry *geo2 = NULL;
  bool is_null_res = false;
  bool result = false;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (OB_FAIL(ObGeoExprUtils::process_input_geometry(srs_guard, expr, ctx, temp_allocator, 
                                  geo1, geo2, is_null_res, srs, N_ST_DISJOINT))) {
    LOG_WARN("fail to process input geometry", K(ret));
  } else if (is_null_res) {
    // do nothing
  } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo1, is_geo1_empty))
             || OB_FAIL(ObGeoExprUtils::check_empty(geo2, is_geo2_empty))) {
    LOG_WARN("check geo empty failed", K(ret));
  } else if (is_geo1_empty || is_geo2_empty) {
    is_null_res = true;
  } else if (OB_FAIL(ObGeoExprUtils::zoom_in_geos_for_relation(*geo1, *geo2))) {
    LOG_WARN("zoom in geos failed", K(ret));
  } else {
    bool disjoint_result = false;
    ObGeoEvalCtx gis_context(&temp_allocator, srs);
    if (OB_FAIL(gis_context.append_geo_arg(geo1)) || OB_FAIL(gis_context.append_geo_arg(geo2))) {
      LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Disjoint>::geo_func::eval(
                    gis_context, disjoint_result))) {
      LOG_WARN("eval st intersection failed", K(ret));
      ObGeoExprUtils::geo_func_error_handle(ret, N_ST_DISJOINT);
    } else {
      result = disjoint_result;
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null_res) {
      res.set_null();
    } else {
      res.set_bool(result);
    }
  }
  return ret;
}

int ObExprSTDisjoint::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_disjoint;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase