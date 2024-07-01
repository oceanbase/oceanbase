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
 * This file contains implementation for eval_sdo_relate.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "lib/geo/ob_geo_func_register.h"
#include "lib/geo/ob_geo_ibin.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_srs.h"
#include "ob_expr_sdo_relate.h"
#include "lib/geo/ob_geo_cache.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprSdoRelate::ObExprSdoRelate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SDO_RELATE, N_SDO_RELATE, 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSdoRelate::~ObExprSdoRelate()
{
}

int ObExprSdoRelate::calc_result_type3(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprResType &type2,
                                       ObExprResType &type3,
                                       common::ObExprTypeCtx &type_ctx) const
{
  INIT_SUCC(ret);

  if (type1.get_type() == ObNullType) {
  } else if (ob_is_nchar(type1.get_type())) {
    ret = OB_ERR_OPERATOR_NOT_EXIST;
    LOG_WARN("operator binding does not exist", K(ret));
  } else if (!ob_is_geometry(type1.get_type()) && !ob_is_string_type(type1.get_type())) {
    type1.set_calc_type(ObVarcharType);
    type1.set_calc_collation_type(CS_TYPE_BINARY);
  }
  if (OB_FAIL(ret)) {
  } else if (type2.get_type() == ObNullType) {
  } else if (ob_is_nchar(type2.get_type())) {
    ret = OB_ERR_OPERATOR_NOT_EXIST;
    LOG_WARN("operator binding does not exist", K(ret));
  } else if (!ob_is_geometry(type2.get_type()) && !ob_is_string_type(type2.get_type())) {
    type2.set_calc_type(ObVarcharType);
    type2.set_calc_collation_type(CS_TYPE_BINARY);
  }
  if (OB_FAIL(ret)) {
  } else if (type3.get_type() == ObNullType ) {
    ret = OB_INVALID_MASK;
    LOG_WARN("invalid mask param", K(ret), K(type3.get_type()));
  } else if (ob_is_blob(type3.get_type(), type3.get_collation_type())
            || ob_is_json(type3.get_type()) || ob_is_user_defined_type(type3.get_type())) {
    ret = OB_ERR_OPERATOR_NOT_EXIST;
    LOG_WARN("operator binding does not exist", K(ret));
  } else if (!ob_is_string_type(type3.get_type())) {
    type3.set_calc_type(ObVarcharType);
    type3.set_calc_collation_type(CS_TYPE_BINARY);
  }
  if (OB_SUCC(ret)) {
    type.set_type(ObObjType::ObVarcharType);
    type.set_collation_type(type_ctx.get_coll_type());
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_full_length(strlen("false") + 1, 1);
  }
  return ret;
}

int ObExprSdoRelate::get_params(ObExpr *param_expr, ObArenaAllocator& temp_allocator, ObEvalCtx &ctx, ObSdoRelateMask& mask)
{
  INIT_SUCC(ret);
  ObDatum *param_datum = nullptr;
  if (OB_ISNULL(param_expr)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret), KP(param_expr));
  } else if (OB_FAIL(param_expr->eval(ctx, param_datum))) {
    LOG_WARN("eval param args failed", K(ret));
  } else if (param_datum->is_null()) {
    ret = OB_INVALID_MASK;
    LOG_WARN("invalid mask param", K(ret));
  } else {
    ObString original_str = param_datum->get_string();
    ObString upper_str;
    if (OB_FAIL(ob_simple_low_to_up(temp_allocator, original_str, upper_str))) {
      LOG_WARN("failed to get upper string", K(ret));
    } else {
      char cmp_str[upper_str.length() + 1];
      cmp_str[upper_str.length()] = '\0';
      MEMCPY(cmp_str, upper_str.ptr(), upper_str.length());
      if (nullptr != strstr(cmp_str, ObSdoRelationship::ANYINTERACT) && OB_FALSE_IT(mask.anyinteract_ = 1)) {
      } else if (mask.anyinteract_ != 1
                && (strstr(cmp_str, ObSdoRelationship::CONTAINS)
                || strstr(cmp_str, ObSdoRelationship::COVEREDBY)
                || strstr(cmp_str, ObSdoRelationship::COVERS)
                || strstr(cmp_str, ObSdoRelationship::EQUAL)
                || strstr(cmp_str, ObSdoRelationship::ON)
                || strstr(cmp_str, ObSdoRelationship::OVERLAPBDYDISJOINT)
                || strstr(cmp_str, ObSdoRelationship::OVERLAPBDYINTERSECT)
                || strstr(cmp_str, ObSdoRelationship::INSIDE)
                || strstr(cmp_str, ObSdoRelationship::TOUCH))) {
        // other spatial relationsh is not supported yet, no need to continue
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not sopported yet.", K(ret));
      }
    }
  }
  return ret;
}

int ObExprSdoRelate::set_relate_result(ObIAllocator &res_alloc, ObDatum &res, bool result)
{
  INIT_SUCC(ret);

  ObString data = result ? "TRUE" : "FALSE";
  ObString str_value;
  char *str_buf = nullptr;
  if (OB_ISNULL(str_buf = static_cast<char*>(res_alloc.alloc(data.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc failed", K(ret));
  } else if (OB_FALSE_IT(str_value.assign_buffer(str_buf, data.length()))) {
  } else if (OB_UNLIKELY(data.length() != str_value.write(data.ptr(), data.length()))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("write amp char failed", K(ret));
  } else {
    res.set_string(str_value);
  }
  return ret;
}

int ObExprSdoRelate::eval_sdo_relate(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObSdoRelateMask mask;
  mask.flags_ = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  bool result = false;
  ObExprStrResAlloc res_alloc(expr, ctx);
  if (ob_is_string_type(expr.args_[2]->datum_meta_.type_) && OB_FAIL(get_params(expr.args_[2], temp_allocator, ctx, mask))) {
    LOG_WARN("fail to get mask param", K(ret), K(mask.flags_));
  } else if (mask.flags_ == 0) {
    ret = set_relate_result(res_alloc, res, false);
  } else {
    ObDatum *gis_datum1 = NULL;
    ObDatum *gis_datum2 = NULL;
    ObExpr *gis_arg1 = expr.args_[0];
    ObExpr *gis_arg2 = expr.args_[1];
    ObObjType input_type1 = gis_arg1->datum_meta_.type_;
    ObObjType input_type2 = gis_arg2->datum_meta_.type_;
    if (OB_FAIL(gis_arg1->eval(ctx, gis_datum1)) || OB_FAIL(gis_arg2->eval(ctx, gis_datum2))) {
      LOG_WARN("eval geo args failed", K(ret));
    } else if (gis_datum1->is_null() || gis_datum2->is_null()) {
      ret = OB_ERR_OPERATOR_NOT_EXIST;
      LOG_WARN("operator binding does not exist", K(ret));
    } else {
      ObDatum *gis_datum1 = NULL;
      ObDatum *gis_datum2 = NULL;
      ObExpr *gis_arg1 = expr.args_[0];
      ObExpr *gis_arg2 = expr.args_[1];
      int num_args = expr.arg_cnt_;
      ObObjType input_type1 = gis_arg1->datum_meta_.type_;
      ObObjType input_type2 = gis_arg2->datum_meta_.type_;
      ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
      common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
      if (OB_FAIL(gis_arg1->eval(ctx, gis_datum1)) || OB_FAIL(gis_arg2->eval(ctx, gis_datum2))) {
        LOG_WARN("eval geo args failed", K(ret));
      } else if (gis_datum1->is_null() || gis_datum2->is_null()) {
        res.set_null();
      } else {
        ObGeoConstParamCache* const_param_cache = ObGeoExprUtils::get_geo_constParam_cache(expr.expr_ctx_id_, &ctx.exec_ctx_);
        bool is_geo1_empty = false;
        bool is_geo2_empty = false;
        ObGeometry *geo1 = NULL;
        ObGeometry *geo2 = NULL;
        ObGeoType type1;
        ObGeoType type2;
        uint32_t srid1;
        uint32_t srid2;
        ObString wkb1 = gis_datum1->get_string();
        ObString wkb2 = gis_datum2->get_string();
        omt::ObSrsCacheGuard srs_guard;
        const ObSrsItem *srs = NULL;
        bool is_geo1_cached = false;
        bool is_geo2_cached = false;
        ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
        bool box_intersects = true;

        if (gis_arg1->is_static_const_) {
          ObGeoExprUtils::expr_get_const_param_cache(const_param_cache, geo1, srid1, is_geo1_cached, 0);
        }
        if (gis_arg2->is_static_const_) {
          ObGeoExprUtils::expr_get_const_param_cache(const_param_cache, geo2, srid2, is_geo2_cached, 1);
        }
        if (!is_geo1_cached && OB_FAIL(ObGeoExprUtils::expr_prepare_build_geometry(temp_allocator, *gis_datum1, *gis_arg1, wkb1, type1, srid1))) {
          if (ret == OB_ERR_GIS_INVALID_DATA) {
            ret = OB_ERR_OPERATOR_NOT_EXIST;
            LOG_WARN("operator binding does not exist", K(ret));
          }
        } else if (!is_geo2_cached && OB_FAIL(ObGeoExprUtils::expr_prepare_build_geometry(temp_allocator, *gis_datum2, *gis_arg2, wkb2, type2, srid2))) {
          if (ret == OB_ERR_GIS_INVALID_DATA) {
            ret = OB_ERR_OPERATOR_NOT_EXIST;
            LOG_WARN("operator binding does not exist", K(ret));
          }
        } else if (srid1 != srid2) {
          if (srid1 == UINT_MAX32 || srid2 == UINT_MAX32) {
            ret = OB_GEO_IN_DIFFERENT_COORDINATE;
            LOG_WARN("srid not the same", K(srid1), K(srid2));
          } else {
            ret = set_relate_result(res_alloc, res, false);
          }
        } else if (OB_ISNULL(session)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get session", K(ret));
        } else if (!is_geo1_cached && !is_geo2_cached && OB_FAIL(ObGeoExprUtils::get_srs_item(session->get_effective_tenant_id(), srs_guard, srid1, srs))) {
          LOG_WARN("fail to get srs item", K(ret), K(srid1));
        } else if (!is_geo1_cached && OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb1, geo1, nullptr, N_ST_INTERSECTS, ObGeoBuildFlag::GEO_ALLOW_3D_CARTESIAN))) {
          LOG_WARN("get first geo by wkb failed", K(ret));
        } else if (!is_geo2_cached && OB_FAIL(ObGeoExprUtils::build_geometry(temp_allocator, wkb2, geo2, nullptr, N_ST_INTERSECTS, ObGeoBuildFlag::GEO_ALLOW_3D_CARTESIAN))) {
          LOG_WARN("get second geo by wkb failed", K(ret));
        } else if ((!is_geo1_cached && OB_FAIL(ObGeoExprUtils::check_empty(geo1, is_geo1_empty)))
            || (!is_geo2_cached && OB_FAIL(ObGeoExprUtils::check_empty(geo2, is_geo2_empty)))) {
          LOG_WARN("check geo empty failed", K(ret));
        } else if (is_geo1_empty || is_geo2_empty) {
          res.set_null();
        } else if (ObGeoTypeUtil::is_3d_geo_type(geo1->type()) || ObGeoTypeUtil::is_3d_geo_type(geo2->type())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only support 2D spatial relationship", K(ret));
        } else if (OB_FAIL(ObGeoExprUtils::zoom_in_geos_for_relation(*geo1, *geo2, is_geo1_cached, is_geo2_cached))) {
          LOG_WARN("zoom in geos failed", K(ret));
        } else {
          // add cache if need
          if (OB_NOT_NULL(const_param_cache)) {
            if (gis_arg1->is_static_const_ && !is_geo1_cached &&
                OB_FAIL(const_param_cache->add_const_param_cache(0, *geo1))) {
              LOG_WARN("add geo1 to const cache failed", K(ret));
            } else if (gis_arg2->is_static_const_ && !is_geo2_cached &&
                OB_FAIL(const_param_cache->add_const_param_cache(1, *geo2))) {
              LOG_WARN("add geo2 to const cache failed", K(ret));
            }
          }
          // check each relation ship, util res is true
          if (OB_FAIL(ret)) {
          } else if (!result && mask.anyinteract_ == 1) {
            bool tmp_result = false;
            if (OB_FAIL(ObGeoExprUtils::get_intersects_res(*geo1, *geo2, gis_arg1, gis_arg2, const_param_cache, srs, temp_allocator, tmp_result))) {
              LOG_WARN("fail to get interact res", K(ret));
            } else {
              result = (result || tmp_result);
            }
          }
          // set result
          if (OB_FAIL(ret)) {
          } else {
            ret = set_relate_result(res_alloc, res, result);
          }
        } // add cache and set res
      }
    }// deal gis param
  }
  return ret;
}

int ObExprSdoRelate::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_sdo_relate;
  return OB_SUCCESS;
}

}
}
