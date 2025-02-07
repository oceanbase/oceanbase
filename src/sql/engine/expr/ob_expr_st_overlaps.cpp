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
 * This file contains implementation for eval_st_overlaps.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_ibin.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_expr_st_overlaps.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSTOverlaps::ObExprSTOverlaps(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_OVERLAPS, N_ST_OVERLAPS, 2,
        VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprSTOverlaps::~ObExprSTOverlaps()
{}

int ObExprSTOverlaps::calc_result_type2(ObExprResType &type, ObExprResType &type1,
    ObExprResType &type2, common::ObExprTypeCtx &type_ctx) const
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
  type.set_int();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  return ret;
}

int ObExprSTOverlaps::process_input_geometry(omt::ObSrsCacheGuard &srs_guard, const ObExpr &expr, ObEvalCtx &ctx,
    MultimodeAlloctor &allocator, ObGeometry *&geo1, ObGeometry *&geo2, bool &is_null_res,
    const ObSrsItem *&srs)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum1 = NULL;
  ObDatum *gis_datum2 = NULL;
  ObExpr *gis_arg1 = expr.args_[0];
  ObExpr *gis_arg2 = expr.args_[1];
  ObObjType input_type1 = gis_arg1->datum_meta_.type_;
  ObObjType input_type2 = gis_arg2->datum_meta_.type_;
  is_null_res = false;
  if (OB_FAIL(allocator.eval_arg(gis_arg1, ctx, gis_datum1)) || OB_FAIL(allocator.eval_arg(gis_arg2, ctx, gis_datum2))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (gis_datum1->is_null() || gis_datum2->is_null()) {
    is_null_res = true;
  } else if (input_type1 == ObIntType || input_type2 == ObIntType) {
    // bugfix 53283098, should allow int type in calc_result_type2
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_CROSSES);
    LOG_WARN("invalid type", K(ret), K(input_type1), K(input_type2));
  } else {
    ObGeoType type1;
    ObGeoType type2;
    uint32_t srid1;
    uint32_t srid2;
    ObString wkb1 = gis_datum1->get_string();
    ObString wkb2 = gis_datum2->get_string();
    bool is_geo1_valid = false;
    bool is_geo2_valid = false;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data_with_copy(allocator,
            *gis_datum1,
            gis_arg1->datum_meta_,
            gis_arg1->obj_meta_.has_lob_header(),
            wkb1))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb1));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data_with_copy(allocator,
                   *gis_datum2,
                   gis_arg2->datum_meta_,
                   gis_arg2->obj_meta_.has_lob_header(),
                   wkb2))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb2));
    } else if (FALSE_IT(allocator.set_baseline_size(wkb1.length() + wkb2.length()))) {
    } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb1, type1, srid1))) {
      if (ret == OB_ERR_GIS_INVALID_DATA) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_OVERLAPS);
      }
      LOG_WARN("get type and srid from wkb failed", K(wkb1), K(ret));
    } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb2, type2, srid2))) {
      if (ret == OB_ERR_GIS_INVALID_DATA) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_OVERLAPS);
      }
      LOG_WARN("get type and srid from wkb failed", K(wkb2), K(ret));
    } else if (srid1 != srid2) {
      ret = OB_ERR_GIS_DIFFERENT_SRIDS;
      LOG_WARN("srid not the same", K(ret), K(srid1), K(srid2));
      LOG_USER_ERROR(OB_ERR_GIS_DIFFERENT_SRIDS, N_ST_OVERLAPS, srid1, srid2);
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(
                   ctx, srs_guard, wkb1, srs, true, N_ST_OVERLAPS))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb1));
    } else if (OB_FAIL(
                   ObGeoExprUtils::build_geometry(allocator, wkb1, geo1, srs, N_ST_OVERLAPS, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT | GEO_NOT_COPY_WKB))) {
      LOG_WARN("get first geo by wkb failed", K(ret));
    } else if (OB_FAIL(
                   ObGeoExprUtils::build_geometry(allocator, wkb2, geo2, srs, N_ST_OVERLAPS, ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT | GEO_NOT_COPY_WKB))) {
      LOG_WARN("get second geo by wkb failed", K(ret));
    }
  }
  return ret;
}

int ObExprSTOverlaps::eval_st_overlaps(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_geo1_empty = false;
  bool is_geo2_empty = false;
  ObGeometry *geo1 = NULL;
  ObGeometry *geo2 = NULL;
  bool is_null_res = false;
  bool result = false;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret, N_ST_OVERLAPS);
  if (OB_FAIL(process_input_geometry(srs_guard, expr, ctx, temp_allocator, geo1, geo2, is_null_res, srs))) {
    LOG_WARN("fail to process input geometry", K(ret));
  }
  ObGeoBoostAllocGuard guard(tenant_id);
  lib::MemoryContext *mem_ctx = nullptr;
  if (OB_FAIL(ret) || is_null_res) {
    // do nothing
  } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo1, is_geo1_empty))
             || OB_FAIL(ObGeoExprUtils::check_empty(geo2, is_geo2_empty))) {
    LOG_WARN("check geo empty failed", K(ret));
  } else if (is_geo1_empty || is_geo2_empty) {
    is_null_res = true;
  } else if (OB_FAIL(ObGeoExprUtils::zoom_in_geos_for_relation(srs, *geo1, *geo2))) {
    LOG_WARN("zoom in geos failed", K(ret));
  } else if (OB_FAIL(guard.init())) {
    LOG_WARN("fail to init geo allocator guard", K(ret));
  } else if (OB_ISNULL(mem_ctx = guard.get_memory_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to get mem ctx", K(ret));
  } else {
    ObGeoFuncResWithNull overlaps_result;
    ObGeoEvalCtx gis_context(*mem_ctx, srs);
    if (OB_FAIL(gis_context.append_geo_arg(geo1)) || OB_FAIL(gis_context.append_geo_arg(geo2))) {
      LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Overlaps>::geo_func::eval(
                    gis_context, overlaps_result))) {
      LOG_WARN("eval st intersection failed", K(ret));
      ObGeoExprUtils::geo_func_error_handle(ret, N_ST_OVERLAPS);
    } else if (overlaps_result.is_null) {
      is_null_res = true;
    } else {
      result = overlaps_result.bret;
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null_res) {
      res.set_null();
    } else {
      res.set_bool(result);
    }
  }
  if (mem_ctx != nullptr) {
    temp_allocator.add_ext_used((*mem_ctx)->arena_used());
  }
  return ret;
}

int ObExprSTOverlaps::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_overlaps;
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
