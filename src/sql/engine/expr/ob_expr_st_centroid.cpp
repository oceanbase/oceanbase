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
 * This file contains implementation for ob_expr_st_centroid.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_st_centroid.h"
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{

ObExprSTCentroid::ObExprSTCentroid(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_CENTROID, N_ST_CENTROID, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTCentroid::~ObExprSTCentroid()
{
}

int ObExprSTCentroid::calc_result_type1(ObExprResType &type,
                                    ObExprResType &type1,
                                    common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (ob_is_numeric_type(type1.get_type())) {
    type1.set_calc_type(ObLongTextType);
  } else if (!ob_is_geometry(type1.get_type())
             && !ob_is_string_type(type1.get_type())
             && type1.get_type() != ObNullType) {
    // handle string types as hex strings(wkb)
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_CENTROID);
    LOG_WARN("invalid type", K(ret), K(type1.get_type()));
  } else {
    type.set_geometry();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  }
  return ret;
}

int ObExprSTCentroid::eval_st_centroid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum = NULL;
  ObExpr *gis_arg = expr.args_[0];
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObObjType input_type = gis_arg->datum_meta_.type_;

  if (OB_FAIL(gis_arg->eval(ctx, gis_datum))) {
    LOG_WARN("eval geo arg failed", K(ret));
  } else if (gis_datum->is_null()) {
    res.set_null();
  } else {
    ObGeoWkbByteOrder bo = ObGeoWkbByteOrder::LittleEndian;
    ObGeometry *geo = NULL;
    omt::ObSrsCacheGuard srs_guard;
    const ObSrsItem *srs = NULL;
    ObString wkb = gis_datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum,
        gis_arg->datum_meta_, gis_arg->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb));
    } else if (ObGeoTypeUtil::get_bo_from_wkb(wkb, bo)) {
      LOG_WARN("fail to get byte order", K(ret), K(wkb));
    } else if (OB_UNLIKELY(ObGeoWkbByteOrder::LittleEndian != bo)) {
      ret = OB_ERR_GIS_DATA_WRONG_ENDIANESS;
      LOG_WARN("byte order is not little endian", K(ret), K(bo));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs, true, N_ST_CENTROID))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb, geo, srs, N_ST_CENTROID))) {
      LOG_WARN("get geo by wkb failed", K(ret));
    } else {
      ObGeoEvalCtx gis_context(&temp_allocator, srs);
      ObCartesianPoint point;
      ObString res_wkb;
      if (OB_FAIL(gis_context.append_geo_arg(geo))) {
        LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
      } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Centroid>::geo_func::eval(gis_context, point))) {
        if (OB_LIKELY(OB_EMPTY_RESULT == ret)) {
          ret = OB_SUCCESS;
          res.set_null();
        } else if (OB_LIKELY(OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS == ret)) {
          LOG_USER_ERROR(OB_ERR_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS, N_ST_CENTROID, ObGeoTypeUtil::get_geo_name_by_type(geo->type()));
        } else {
          LOG_WARN("eval st centroid failed", K(ret));
          ObGeoExprUtils::geo_func_error_handle(ret, N_ST_CENTROID);
        }
      } else if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(point, expr, ctx, srs, res_wkb))) {
        LOG_WARN("failed to write geometry to wkb", K(ret));
      } else {
        res.set_string(res_wkb);
      }
    }
    }
  return ret;
}

int ObExprSTCentroid::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_centroid;
  return OB_SUCCESS;
}

}
}
