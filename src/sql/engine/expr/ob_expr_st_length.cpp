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
 * This file contains implementation for _st_length.
 */
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_length.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "lib/geo/ob_geo_func_register.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{
ObExprSTLength::ObExprSTLength(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_LENGTH, N_ST_LENGTH, ONE_OR_TWO,
          NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}
ObExprSTLength::~ObExprSTLength()
{}
int ObExprSTLength::calc_result_typeN(ObExprResType &type, ObExprResType *types_stack,
    int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  ObObjType geo_tp = types_stack[0].get_type();
  if (!ob_is_geometry(geo_tp) && !ob_is_string_type(geo_tp) && !ob_is_null(geo_tp)) {
    types_stack[0].set_calc_type(ObVarcharType);
    types_stack[0].set_calc_collation_type(CS_TYPE_BINARY);
  } else if (param_num == 2) {
    ObObjType unit_tp = types_stack[1].get_type();
    if (ob_is_string_type(unit_tp) || ob_is_null(unit_tp)) {
      // do nothing
    } else {
      types_stack[1].set_calc_type(ObVarcharType);
      types_stack[1].set_calc_collation_type(types_stack[1].get_collation_type());
      types_stack[1].set_calc_collation_level(types_stack[1].get_collation_level());
    }
  }
  if (OB_SUCC(ret)) {
    type.set_double();
  }
  return ret;
}

int ObExprSTLength::eval_st_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_null_res = false;
  ObDatum *datum1 = nullptr;
  ObExpr *arg1 = expr.args_[0];
  ObObjType type1 = arg1->datum_meta_.type_;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret, N_ST_LENGTH);
  double res_num = 0;
  ObDatum *gis_unit = NULL;
  if (ob_is_null(type1)) {
    is_null_res = true;
  } else if (OB_FAIL(temp_allocator.eval_arg(arg1, ctx, datum1))) {
    LOG_WARN("fail to eval args", K(ret));
  } else if (datum1->is_null()) {
    is_null_res = true;
  } else if (type1 == ObIntType) {
    // bugfix 53283098, should allow double type in calc_result_type2
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_CROSSES);
    LOG_WARN("invalid type", K(ret), K(type1));
  } else if (expr.arg_cnt_ == 2 && OB_FAIL(temp_allocator.eval_arg(expr.args_[1], ctx, gis_unit))) {
    LOG_WARN("eval geo unit arg failed", K(ret));
  } else if (expr.arg_cnt_ == 2 && gis_unit->is_null()) {
    is_null_res = true;
  } else {
    // construct geometry
    ObString wkb = datum1->get_string();
    ObGeoType gtype = ObGeoType::GEOTYPEMAX;
    ObGeometry *geo = nullptr;
    const ObSrsItem *srs = NULL;
    omt::ObSrsCacheGuard srs_guard;
    ObGeoBoostAllocGuard guard(tenant_id);
    lib::MemoryContext *mem_ctx = nullptr;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data_with_copy(
            temp_allocator, *datum1, arg1->datum_meta_, arg1->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to read real string data", K(ret), K(arg1->obj_meta_.has_lob_header()));
    } else if (FALSE_IT(temp_allocator.set_baseline_size(wkb.length()))) {
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs, true, N_ST_LENGTH))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(
                   temp_allocator, wkb, geo, srs, N_ST_LENGTH, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT | GEO_NOT_COPY_WKB))) {  // ObIWkbGeom
      LOG_WARN("fail to build geometry from wkb", K(ret), K(wkb));
    } else if (geo->type() != ObGeoType::LINESTRING && geo->type() != ObGeoType::MULTILINESTRING) {
      is_null_res = true;
    } else if (OB_FAIL(guard.init())) {
      LOG_WARN("fail to init geo allocator guard", K(ret));
    } else if (OB_ISNULL(mem_ctx = guard.get_memory_ctx())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("fail to get mem ctx", K(ret));
    } else {
      // cal length
      ObGeoEvalCtx gis_context(*mem_ctx, srs);
      if (OB_FAIL(gis_context.append_geo_arg(geo))) {
        LOG_WARN("build gis context failed", K(ret));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Length>::geo_func::eval(gis_context, res_num))) {
        LOG_WARN("eval st distance failed", K(ret));
        if (OB_ERR_GIS_INVALID_DATA == ret) {
          LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_LENGTH);
        } else {
          ObGeoExprUtils::geo_func_error_handle(ret, N_ST_LENGTH);
        }
      } else if (std::isinf(res_num)) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("Length value is out of range in st_length", K(ret));
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "Length", N_ST_LENGTH);
      } else if (expr.arg_cnt_ == 2) {
        // transfer to unit
        if (OB_FAIL(ObGeoExprUtils::length_unit_conversion(
                       gis_unit->get_string(), srs, res_num, res_num))) {
          LOG_WARN("fail to do unit conversion", K(ret), K(res_num));
        } else if (std::isinf(res_num)) {
          ret = OB_OPERATE_OVERFLOW;
          LOG_WARN("Length value is out of range in st_length", K(ret));
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "Length", N_ST_LENGTH);
        }
      }
    }
    if (mem_ctx != nullptr) {
      temp_allocator.add_ext_used((*mem_ctx)->arena_used());
    }
  }
  // set result
  if (OB_SUCC(ret)) {
    if (is_null_res) {
      res.set_null();
    } else {
      res.set_double(res_num);
    }
  }
  return ret;
}
int ObExprSTLength::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_length;
  return OB_SUCCESS;
}
}  // namespace sql
}  // namespace oceanbase