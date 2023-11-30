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
 * This file contains implementation for _st_numinteriorrings.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_numinteriorrings.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "share/object/ob_obj_cast_util.h"
#include "lib/geo/ob_geo_utils.h"
#include "observer/omt/ob_tenant_srs.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTNumInteriorRings::ObExprPrivSTNumInteriorRings(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_NUMINTERIORRINGS, N_PRIV_ST_NUMINTERIORRINGS, 1,
          NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprPrivSTNumInteriorRings::~ObExprPrivSTNumInteriorRings()
{}

int ObExprPrivSTNumInteriorRings::calc_result_type1(
    ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjType obj_type1 = type1.get_type();

  if (!ob_is_string_type(obj_type1) && !ob_is_geometry(obj_type1) && !ob_is_null(obj_type1)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_NUMINTERIORRINGS);
    LOG_WARN("invalid type", K(ret), K(obj_type1));
  } else {
    ObCastMode cast_mode = type_ctx.get_cast_mode();
    cast_mode &= ~CM_WARN_ON_FAIL;      // make cast return error when fail
    type_ctx.set_cast_mode(cast_mode);  // cast mode only do work in new sql engine cast frame.
    type.set_int32();
  }

  return ret;
}

int ObExprPrivSTNumInteriorRings::eval_priv_st_numinteriorrings(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  bool is_null_res = false;
  ObDatum *datum1 = nullptr;
  ObExpr *arg1 = expr.args_[0];
  ObObjType type1 = arg1->datum_meta_.type_;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  uint32_t res_num = 0;

  if (ob_is_null(type1)) {
    is_null_res = true;
  } else if (OB_FAIL(arg1->eval(ctx, datum1))) {
    LOG_WARN("fail to eval args", K(ret));
  } else if (datum1->is_null()) {
    is_null_res = true;
  } else {
    ObString wkb = datum1->get_string();
    ObGeoType gtype = ObGeoType::GEOTYPEMAX;
    ObGeometry *geo = nullptr;

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(
            temp_allocator, *datum1, arg1->datum_meta_, arg1->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to read real string data", K(ret), K(arg1->obj_meta_.has_lob_header()));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator,
                   wkb,
                   geo,
                   nullptr,
                   N_PRIV_ST_NUMINTERIORRINGS,
                   ObGeoBuildFlag::GEO_ALLOW_3D_DEFAULT))) {  // ObIWkbGeom
      LOG_WARN("fail to build geometry from wkb", K(ret), K(wkb));
    } else if (geo->type() != ObGeoType::POLYGON) {
      is_null_res = true;
    } else {
      const ObWkbGeomPolygon *poly = reinterpret_cast<const ObWkbGeomPolygon *>(geo->val());
      if ((res_num = poly->size()) > 0) {
        --res_num;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null_res) {
      res.set_null();
    } else {
      res.set_int32(res_num);
    }
  }

  return ret;
}

int ObExprPrivSTNumInteriorRings::cg_expr(
    ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_numinteriorrings;
  return OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase