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
 * This file contains implementation for eval_st_bestsrid.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_ibin.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "ob_expr_st_bestsrid.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprPrivSTBestsrid::ObExprPrivSTBestsrid(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_BESTSRID, N_PRIV_ST_BESTSRID, ONE_OR_TWO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprPrivSTBestsrid::~ObExprPrivSTBestsrid()
{
}

int ObExprPrivSTBestsrid::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (OB_UNLIKELY(param_num != 2 && param_num != 1)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  } else {
    for (int64_t i = 0; i < param_num && OB_SUCC(ret); i++) {
      if (types_stack[i].get_type() != ObGeometryType
          && !ob_is_string_type(types_stack[i].get_type())
          && types_stack[i].get_type() != ObNullType) {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("invalid type", K(ret), K(types_stack[i].get_type()));
      }
    }
    if (OB_SUCC(ret)) {
      type.set_int32();
      type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
      type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
    }
  }
  return ret;
}

int ObExprPrivSTBestsrid::get_geog_box(ObEvalCtx &ctx, ObArenaAllocator &temp_allocator, ObString wkb,
                                       ObObjType input_type, bool &is_geo_empty, ObGeogBox *&geo_box)
{
  int ret = OB_SUCCESS;
  ObGeometry *geo = NULL;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = NULL;
  if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs))) {
    LOG_WARN("get srs failed", K(ret), K(wkb));
  } else if (OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb, geo, srs, N_PRIV_ST_BESTSRID,
                                                    ObGeoBuildFlag::GEO_ALLOW_3D))) {
    LOG_WARN("get geo failed", K(ret));
    if (ret != OB_ERR_SRS_NOT_FOUND && ret != OB_ERR_INVALID_GEOMETRY_TYPE) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_BESTSRID);
    }
  } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo, is_geo_empty))) {
    LOG_WARN("check geo empty failed", K(ret));
  } else if (is_geo_empty) {
    // do nothing
  } else if (ob_is_string_type(input_type)
      && OB_FAIL(ObGeoExprUtils::check_coordinate_range(srs, geo, N_PRIV_ST_BESTSRID, true))) {
    LOG_WARN("invalid coordinate range", K(input_type), K(geo));
  } else if (geo->crs() != ObGeoCRS::Geographic) {
    ret = OB_ERR_NOT_IMPLEMENTED_FOR_PROJECTED_SRS;
    LOG_USER_ERROR(OB_ERR_NOT_IMPLEMENTED_FOR_PROJECTED_SRS, N_PRIV_ST_BESTSRID,
                  ObGeoTypeUtil::get_geo_name_by_type(geo->type()));
  } else {
    ObGeoEvalCtx gis_context(&temp_allocator, srs);
    ObGeogBox *result = NULL;
    if (OB_FAIL(gis_context.append_geo_arg(geo))) {
      LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Box>::geo_func::eval(gis_context, result))) {
      LOG_WARN("failed to do box functor failed", K(ret));
    } else {
      geo_box = result;
    }
  }
  return ret;
}

int ObExprPrivSTBestsrid::eval_st_bestsrid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObGeogBox *geo_box1 = NULL;
  ObGeogBox *geo_box2 = NULL;
  uint32_t param_num = expr.arg_cnt_;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  omt::ObSrsCacheGuard srs_guard;
  bool is_null_res = false;
  bool is_geo_empty = false;

  for (uint8_t i = 0; i < param_num && OB_SUCC(ret); i++) {
    ObDatum *geo_datum = NULL;
    ObString geo_str;
    ObExpr *geo_arg = expr.args_[i];
    ObObjType input_type = geo_arg->datum_meta_.type_;
    if (OB_FAIL(geo_arg->eval(ctx, geo_datum))) {
      LOG_WARN("eval geo args failed", K(ret));
    } else if (geo_datum->is_null()) {
      res.set_null();
      is_null_res = true;
    } else if (FALSE_IT(geo_str = geo_datum->get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *geo_datum,
              geo_arg->datum_meta_, geo_arg->obj_meta_.has_lob_header(), geo_str))) {
      LOG_WARN("fail to get real string data", K(ret), K(geo_str));
    } else if (OB_FAIL(ObExprPrivSTBestsrid::get_geog_box(ctx, temp_allocator,
                                                          geo_str,
                                                          input_type,
                                                          is_geo_empty,
                                                          i == 0 ? geo_box1 : geo_box2))) {
      LOG_WARN("get geog box failed", K(ret), K(i));
    }
  }

  if (OB_SUCC(ret) && !is_null_res) {
    int32_t bestsrid = SRID_WORLD_MERCATOR_PG;
    if (!is_geo_empty && OB_FAIL(ObGeoExprUtils::get_box_bestsrid(geo_box1, geo_box2, bestsrid))) {
      LOG_WARN("failed to get box bestsrid", K(ret), KP(geo_box1), KP(geo_box2));
    } else {
      res.set_int(bestsrid);
    }
  }
  return ret;
}

int ObExprPrivSTBestsrid::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_bestsrid;
  return OB_SUCCESS;
}

}
}
