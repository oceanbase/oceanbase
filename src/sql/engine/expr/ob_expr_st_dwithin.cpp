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
 * This file contains implementation for _st_dwithin.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_ibin.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_expr_st_dwithin.h"
#include "lib/geo/ob_geo_utils.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTDWithin::ObExprPrivSTDWithin(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_DWITHIN, N_PRIV_ST_DWITHIN, 3, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprPrivSTDWithin::~ObExprPrivSTDWithin()
{
}

int ObExprPrivSTDWithin::calc_result_type3(ObExprResType &type,
                                           ObExprResType &input1,
                                           ObExprResType &input2,
                                           ObExprResType &input3,
                                           common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  int unexpected_types = 0;
  int null_types = 0;

  if (input1.get_type() == ObNullType) {
    null_types++;
  } else if (!ob_is_geometry(input1.get_type()) && !ob_is_string_type(input1.get_type())) {
    unexpected_types++;
    LOG_WARN("invalid type", K(input1.get_type()));
  }
  if (input2.get_type() == ObNullType) {
    null_types++;
  } else if (!ob_is_geometry(input2.get_type()) && !ob_is_string_type(input2.get_type())) {
    unexpected_types++;
    LOG_WARN("invalid type", K(input2.get_type()));
  }
  // an invalid type and a null type will return null
  // an invalid type and a valid type return error
  if (null_types == 0 && unexpected_types > 0) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_DWITHIN);
      LOG_WARN("invalid type", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (!ob_is_double_type(input3.get_type())) {
      input3.set_calc_type(ObDoubleType);
    }
    type.set_int32();
    type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
    type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }

  return ret;
}

template<typename ResType>
int ObExprPrivSTDWithin::eval_st_dwithin_common(ObEvalCtx &ctx,
                                                ObArenaAllocator &temp_allocator,
                                                ObString wkb1,
                                                ObString wkb2,
                                                double distance_tolerance,
                                                ObObjType input_type1,
                                                ObObjType input_type2,
                                                ResType &res)
{
  int ret = OB_SUCCESS;
  bool is_geo1_empty = false;
  bool is_geo2_empty = false;
  ObGeometry *geo1 = NULL;
  ObGeometry *geo2 = NULL;
  ObGeoType type1;
  ObGeoType type2;
  uint32_t srid1;
  uint32_t srid2;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = NULL;

  if (distance_tolerance < 0.0) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_DWITHIN);
    LOG_WARN("Tolerance cannot be less than zero", K(ret), K(distance_tolerance));
  } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb1, type1, srid1))) {
    LOG_WARN("get type and srid from wkb failed", K(wkb1), K(ret));
    if (ret == OB_ERR_GIS_INVALID_DATA) {
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_DWITHIN);
    }
  } else if (OB_FAIL(ObGeoTypeUtil::get_type_srid_from_wkb(wkb2, type2, srid2))) {
    LOG_WARN("get type and srid from wkb failed", K(wkb2), K(ret));
  } else if (srid1 != srid2) {
    ret = OB_ERR_GIS_DIFFERENT_SRIDS;
    LOG_WARN("srid not the same", K(srid1), K(srid2), K(ret));
  } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb1, srs))) {
    LOG_WARN("fail to get srs item", K(ret), K(wkb1));
  } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_wkb(temp_allocator, wkb1, srs, geo1))) {
    LOG_WARN("get first geo by wkb failed", K(ret));
    if (ret != OB_ERR_SRS_NOT_FOUND && ret != OB_ERR_INVALID_GEOMETRY_TYPE) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_DWITHIN);
    }
  } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_wkb(temp_allocator, wkb2, srs, geo2))) {
    LOG_WARN("get second geo by wkb failed", K(ret));
    if (ret != OB_ERR_SRS_NOT_FOUND && ret != OB_ERR_INVALID_GEOMETRY_TYPE) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_DWITHIN);
    }
  } else if (OB_FAIL(ObGeoExprUtils::check_empty(geo1, is_geo1_empty))
      || OB_FAIL(ObGeoExprUtils::check_empty(geo2, is_geo2_empty))) {
    LOG_WARN("check geo empty failed", K(ret));
  } else if (is_geo1_empty || is_geo2_empty) {
    res.set_null();
  } else if (ob_is_string_type(input_type1)
      && OB_FAIL(ObGeoExprUtils::check_coordinate_range(srs, geo1, N_PRIV_ST_DWITHIN, true))) {
    LOG_WARN("invalid coordinate range", K(input_type1), K(geo1));
  } else if (ob_is_string_type(input_type2)
      && OB_FAIL(ObGeoExprUtils::check_coordinate_range(srs, geo2, N_PRIV_ST_DWITHIN, true))) {
    LOG_WARN("invalid coordinate range", K(input_type2), K(geo2));
  } else {
    ObGeoEvalCtx gis_context(&temp_allocator, srs);
    double result = 0.0;
    if (OB_FAIL(ObGeoExprUtils::normalize_wkb(srs, wkb1, temp_allocator, geo1))) {
      LOG_WARN("normalize wkb1 failed", K(srid1), K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::normalize_wkb(srs, wkb2, temp_allocator, geo2))) {
      LOG_WARN("normalize wkb2 failed", K(srid2), K(ret));
    } else if (OB_FAIL(gis_context.append_geo_arg(geo1)) || OB_FAIL(gis_context.append_geo_arg(geo2))) {
      LOG_WARN("build gis context failed", K(ret), K(gis_context.get_geo_count()));
    } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Distance>::geo_func::eval(gis_context, result))) {
      LOG_WARN("eval st intersection failed", K(ret));
      ObGeoExprUtils::geo_func_error_handle(ret, N_PRIV_ST_DWITHIN);
    } else {
      res.set_bool(result <= distance_tolerance);
    }
  }
  return ret;
}

int ObExprPrivSTDWithin::eval_st_dwithin(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum1 = NULL;
  ObDatum *gis_datum2 = NULL;
  ObDatum *gis_datum3 = NULL;
  ObString wkb1;
  ObString wkb2;
  ObExpr *gis_arg1 = expr.args_[0];
  ObExpr *gis_arg2 = expr.args_[1];
  ObExpr *gis_arg3 = expr.args_[2];
  ObObjType input_type1 = gis_arg1->datum_meta_.type_;
  ObObjType input_type2 = gis_arg2->datum_meta_.type_;
  bool is_null_res = false;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (OB_FAIL(gis_arg1->eval(ctx, gis_datum1)) || OB_FAIL(gis_arg2->eval(ctx, gis_datum2))
      || OB_FAIL(gis_arg3->eval(ctx, gis_datum3))) {
    LOG_WARN("eval geo args failed", K(ret), KP(gis_datum1),KP(gis_datum2),KP(gis_datum3));
  } else if (gis_datum1->is_null() || gis_datum2->is_null() || gis_datum3->is_null()) {
    res.set_null();
  } else if (FALSE_IT(wkb1 = gis_datum1->get_string())) {
  } else if (FALSE_IT(wkb2 = gis_datum2->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum1,
            gis_arg1->datum_meta_, gis_arg1->obj_meta_.has_lob_header(), wkb1))) {
    LOG_WARN("fail to get real string data", K(ret), K(wkb1));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum2,
            gis_arg2->datum_meta_, gis_arg2->obj_meta_.has_lob_header(), wkb2))) {
    LOG_WARN("fail to get real string data", K(ret), K(wkb2));
  } else if (OB_FAIL(ObExprPrivSTDWithin::eval_st_dwithin_common(ctx,
                                                                 temp_allocator,
                                                                 wkb1,
                                                                 wkb2,
                                                                 gis_datum3->get_double(),
                                                                 input_type1,
                                                                 input_type2,
                                                                 res))) {
    LOG_WARN("eval st dwithin failed", K(ret));
  }
  return ret;
}

int ObExprPrivSTDWithin::cg_expr(ObExprCGCtx &expr_cg_ctx,
                             const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_dwithin;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase