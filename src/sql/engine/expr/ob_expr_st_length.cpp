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
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "share/object/ob_obj_cast_util.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_func_register.h"
#include "observer/omt/ob_tenant_srs.h"
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
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  double res_num = 0;
  if (ob_is_null(type1)) {
    is_null_res = true;
  } else if (OB_FAIL(arg1->eval(ctx, datum1))) {
    LOG_WARN("fail to eval args", K(ret));
  } else if (datum1->is_null()) {
    is_null_res = true;
  } else if (type1 == ObIntType) {
    // bugfix 53283098, should allow double type in calc_result_type2
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_CROSSES);
    LOG_WARN("invalid type", K(ret), K(type1));
  } else {
    // construct geometry
    ObString wkb = datum1->get_string();
    ObGeoType gtype = ObGeoType::GEOTYPEMAX;
    ObGeometry *geo = nullptr;
    const ObSrsItem *srs = NULL;
    omt::ObSrsCacheGuard srs_guard;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
            temp_allocator, *datum1, arg1->datum_meta_, arg1->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to read real string data", K(ret), K(arg1->obj_meta_.has_lob_header()));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs, true, N_ST_LENGTH))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(
                   temp_allocator, wkb, geo, srs, N_ST_LENGTH, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {  // ObIWkbGeom
      LOG_WARN("fail to build geometry from wkb", K(ret), K(wkb));
    } else if (geo->type() != ObGeoType::LINESTRING && geo->type() != ObGeoType::MULTILINESTRING) {
      is_null_res = true;
    } else {
      // cal length
      ObGeoEvalCtx gis_context(&temp_allocator, srs);
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
        ObDatum *gis_unit = NULL;
        if (OB_FAIL(expr.args_[1]->eval(ctx, gis_unit))) {
          LOG_WARN("eval geo unit arg failed", K(ret));
        } else if (gis_unit->is_null()) {
          is_null_res = true;
        } else if (OB_FAIL(ObGeoExprUtils::length_unit_conversion(
                       gis_unit->get_string(), srs, res_num, res_num))) {
          LOG_WARN("fail to do unit conversion", K(ret), K(res_num));
        } else if (std::isinf(res_num)) {
          ret = OB_OPERATE_OVERFLOW;
          LOG_WARN("Length value is out of range in st_length", K(ret));
          LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "Length", N_ST_LENGTH);
        }
      }
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