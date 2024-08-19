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
 * This file contains implementation for st_pointn.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "ob_expr_st_pointn.h"
#include "lib/geo/ob_srs_info.h"
#include "observer/omt/ob_tenant_srs.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprSTPointN::ObExprSTPointN(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_POINTN, N_ST_POINTN, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprSTPointN::calc_result_type2(ObExprResType &type,
                                         ObExprResType &type1,
                                         ObExprResType &type2,
                                         common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  ObObjType type_g = type1.get_type();
  ObObjType type_n = type2.get_type();

  if (ob_is_null(type_g)) {
    // do nothing
  } else if (ob_is_numeric_type(type_g)) {
    type1.set_calc_type(ObLongTextType);
  } else if (!ob_is_geometry(type_g) && !ob_is_string_type(type_g)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_name());
  }

  if (OB_SUCC(ret) && !ob_is_null(type_n)) {
    type2.set_calc_type(ObIntType);
  }
  type.set_geometry();
  type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());

  return ret;
}

int ObExprSTPointN::eval_st_pointn(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum = NULL;
  ObDatum *datum2 = NULL;
  ObGeometry *src_geo = NULL;
  ObGeometry *dest_geo = NULL;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsItem *srs = NULL;
  uint32_t srid = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_alloc = tmp_alloc_g.get_allocator();
  bool is_null_result = false;

  if (ob_is_null(expr.args_[0]->datum_meta_.type_) 
      && !ob_is_null(expr.args_[1]->datum_meta_.type_)) {
    is_null_result = true;
  } else if (ob_is_null(expr.args_[0]->datum_meta_.type_) 
            || ob_is_null(expr.args_[1]->datum_meta_.type_)) {
    is_null_result = true;
    ret = OB_ERR_PARAM_SIZE;    
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, gis_datum))) {
    LOG_WARN("eval geo arg failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, datum2))) {
    LOG_WARN("eval index arg failed", K(ret));
  } else if (gis_datum->is_null() || datum2->is_null()) {
    is_null_result = true;
    ret = OB_ERR_PARAM_SIZE;
  } else {
    ObString wkb = gis_datum->get_string();

    if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_alloc, *gis_datum,
              expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs, true, N_ST_POINTN))) {
      LOG_WARN("fail to get srs item", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(wkb, srid))) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_POINTN);
      LOG_WARN("get srid from wkb failed", K(wkb), K(ret));
    } else if (OB_FAIL(ObGeoExprUtils::build_geometry(tmp_alloc, wkb, src_geo, srs, N_ST_POINTN, 
                                                      ObGeoBuildFlag::GEO_DEFAULT))) {
      LOG_WARN("failed to parse wkb", K(ret));        // ObIWkbGeom
    } else if ((src_geo->type() <= ObGeoType::GEOMETRY) 
                || (src_geo->type() >= ObGeoType::GEOTYPEMAX)) {
      ret = OB_ERR_INVALID_GEOMETRY_TYPE;
      LOG_WARN("unknown geometry type", K(ret), K(src_geo->type()));
    } else if (src_geo->type() != ObGeoType::LINESTRING) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("The type of geometry should be LINESTRING", K(ret));
    } else {
      const int N = datum2->get_int() - 1;
      if (N >= 0) { // N should be a no-negative number
        bool is_geog = (src_geo->crs() == ObGeoCRS::Geographic);
        
        // todo

      } else { // N < 0, return NULL
        is_null_result = true;
      }
    }
  }
  if (is_null_result) {
    res.set_null();
  } else if (OB_SUCC(ret)) {
    ObString res_wkb;
    if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*dest_geo, expr, ctx, 
                                            srs, res_wkb, srid))){
      LOG_WARN("failed to write geometry to wkb", K(ret));
    } else {
      res.set_string(res_wkb);
    }
  }
  return ret;
}

template<typename MLS, typename PT>
int ObExprSTPointN::get_sub_point(const ObGeometry *g,
                                  const int N,
                                  bool& is_null_result,
                                  ObIAllocator &allocator,
                                  ObGeometry *&sub_geo) 
{
  int ret = OB_SUCCESS;
  const MLS *src_geo = reinterpret_cast<const MLS *>(const_cast<char *>(g->val()));
  if (N >= src_geo->size()) {
    is_null_result = true;
  } else {
    // typename MLS::iterator iter = src_geo->begin();
    // for (uint32 i = 0; i < N; ++i) {
    //   iter++;
    // }
    // typename MLS::const_pointer sub_ptr = iter.operator->();
    // PT *pt = OB_NEWx(PT, &allocator, 
    //                 sub_ptr->template get<0>(),
    //                 sub_ptr->template get<1>(), 
    //                 g->get_srid(), &allocator);
    // if (OB_ISNULL(pt)) {
    //   ret = OB_ALLOCATE_MEMORY_FAILED;
    //   LOG_WARN("fail to allocate memory", K(ret));
    // } else {
    //   sub_geo = pt;
    // }    
  }
  return ret;
}

int ObExprSTPointN::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                              const ObRawExpr &raw_expr, 
                              ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_pointn;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase