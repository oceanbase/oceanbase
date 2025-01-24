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
 * This file contains implementation for st_valid.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "ob_expr_st_isvalid.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprSTIsValid::ObExprSTIsValid(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_ISVALID, N_ST_ISVALID, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprSTIsValid::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (ob_is_null(type1.get_type())) {
    // do nothing
  } else if (ob_is_numeric_type(type1.get_type())) {
    type1.set_calc_type(ObLongTextType);
  } else if (!ob_is_geometry(type1.get_type()) && !ob_is_string_type(type1.get_type())) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_name());
  }
  type.set_int32();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  return ret;
}

int ObExprSTIsValid::eval_st_isvalid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
  MultimodeAlloctor tmp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret, N_ST_ISVALID);
  ObDatum *datum = NULL;
  int num_args = expr.arg_cnt_;
  bool is_null_result = false;
  ObGeoSrid srid = 0;
  ObString wkb;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = NULL;
  ObGeometry *geo = NULL;
  bool is_geog = false;
  bool isvalid_res = false;

  if (OB_FAIL(tmp_allocator.eval_arg(expr.args_[0], ctx, datum))) {
    LOG_WARN("failed to eval first argument", K(ret));
  } else if (datum->is_null()) {
    is_null_result = true;
  } else {
    wkb = datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data_with_copy(tmp_allocator, *datum,
              expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb));
    } else if (FALSE_IT(tmp_allocator.set_baseline_size(wkb.length()))) {
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs, true, N_ST_ISVALID))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(tmp_allocator, wkb, geo, srs, N_ST_ISVALID, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT | GEO_NOT_COPY_WKB))) {
      LOG_WARN("failed to parse wkb", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObGeoBoostAllocGuard guard(tenant_id);
    lib::MemoryContext *mem_ctx = nullptr;
    if (is_null_result) {
      res.set_null();
    } else if (OB_FAIL(guard.init())) {
      LOG_WARN("fail to init geo allocator guard", K(ret));
    } else if (OB_ISNULL(mem_ctx = guard.get_memory_ctx())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("fail to get mem ctx", K(ret));
    } else {
      ObGeoEvalCtx gis_context(*mem_ctx, srs);
      if (OB_FAIL(gis_context.append_geo_arg(geo))) {
        LOG_WARN("build geo gis context failed", K(ret));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::IsValid>::geo_func::eval(gis_context, isvalid_res))) {
        LOG_WARN("eval geo func isvalid failed", K(ret));
        ObGeoExprUtils::geo_func_error_handle(ret, N_ST_ISVALID);
      } else {
        res.set_bool(isvalid_res);
      }
    }
    if (mem_ctx != nullptr) {
      tmp_allocator.add_ext_used((*mem_ctx)->arena_used());
    }
  }

  return ret;
}

int ObExprSTIsValid::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_isvalid;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase