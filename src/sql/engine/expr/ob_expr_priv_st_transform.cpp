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
 * This file contains implementation for _st_transform.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "lib/geo/ob_geo_func_register.h"
#include "ob_expr_priv_st_transform.h"
#include "lib/geo/ob_srs_info.h"
#include "observer/omt/ob_tenant_srs.h"
#include "lib/geo/ob_geo_normalize_visitor.h"
#include "lib/geo/ob_geo_wkb_size_visitor.h"
#include "lib/geo/ob_geo_wkb_visitor.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprPrivSTTransform::ObExprPrivSTTransform(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_TRANSFORM, N_PRIV_ST_TRANSFORM, TWO_OR_THREE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprPrivSTTransform::calc_result_typeN(ObExprResType& type,
                                             ObExprResType* types_stack,
                                             int64_t param_num,
                                             ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  const ObObjType &type1 = types_stack[0].get_type();
  if (!ob_is_string_type(type1) &&
      !ob_is_geometry(type1) &&
      !ob_is_null(type1)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_TRANSFORM);
    LOG_WARN("invalid type", K(ret), K(type1));
  } else {
    for (int i = 1; i < param_num; i++) {
      if (!ob_is_string_type(types_stack[i].get_type()) && !ob_is_null(types_stack[i].get_type())) {
        if (i == 1 && param_num == 3) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_TRANSFORM);
          LOG_WARN("invalid function format for _st_transform", K(ret), K(type1));
        } else if (!ob_is_integer_type(types_stack[i].get_type())) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_TRANSFORM);
          LOG_WARN("invalid function format for _st_transform", K(ret), K(type1));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObCastMode cast_mode = type_ctx.get_cast_mode();
      cast_mode &= ~CM_WARN_ON_FAIL; // make cast return error when fail
      type.set_type(ObGeometryType);
      type.set_collation_level(common::CS_LEVEL_COERCIBLE);
      type.set_collation_type(CS_TYPE_BINARY);
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
    }
  }

  return ret;
}

int ObExprPrivSTTransform::eval_priv_st_transform(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *gis_datum = NULL;
  ObDatum *datum2 = NULL;
  ObGeometry *src_geo = NULL;
  ObGeometry *dest_geo = NULL;
  omt::ObSrsCacheGuard srs_guard;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  const ObSrsItem *src_srs_item = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  int64_t param_num = expr.arg_cnt_;
  ObString src_proj4_param;
  ObString dest_proj4_param;
  bool need_eval = true;

  if (expr.args_[1]->is_boolean_ || (param_num > 2 && expr.args_[2]->is_boolean_)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid type", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, gis_datum))) {
    LOG_WARN("eval geo arg failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, datum2))) {
    LOG_WARN("eval sird arg failed", K(ret));
  } else if (gis_datum->is_null() || datum2->is_null()) {
    res.set_null();
  } else {
    uint32_t src_srid = 0;
    uint32_t dest_srid = 0;
    ObString wkb = gis_datum->get_string();
    const ObSrsItem *dest_srs_item = NULL;
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *gis_datum,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
      LOG_WARN("fail to get real data.", K(ret), K(wkb));
    } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(wkb, src_srid))) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_TRANSFORM);
      LOG_WARN("get srid from wkb failed", K(wkb), K(ret));
    } else if (OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
      LOG_WARN("get tenant srs guard failed", K(session->get_effective_tenant_id()), K(src_srid), K(ret));
    } else if (src_srid != 0 && OB_FAIL(srs_guard.get_srs_item(src_srid, src_srs_item))) {
      LOG_WARN("failed to get srs item", K(ret), K(src_srid));
    } else if (OB_FAIL(ObGeoTypeUtil::create_geo_by_wkb(temp_allocator, wkb, src_srs_item, src_geo))) {
      LOG_WARN("get geo by wkb failed", K(ret), K(wkb));
      if (ret != OB_ERR_SRS_NOT_FOUND) {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_TRANSFORM);
      }
    } else {
      if (param_num == 2) {
        if (ob_is_integer_type(expr.args_[1]->datum_meta_.type_)) {
          if (datum2->get_int() < 0 || datum2->get_int() > UINT_MAX32) {
            ret = OB_OPERATE_OVERFLOW;
            LOG_WARN("srid input value out of range", K(ret), K(dest_srid));
          }
          dest_srid = datum2->get_int();
          if (OB_SUCC(ret)){
            if (dest_srid == 0) {
              ret = OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED, dest_srid);
            } else if (src_srid == 0) {
              ret = OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED, src_srid);
            } else if (dest_srid == src_srid) { // return src geo directly
              ObString res_wkb;
              if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*src_geo, expr, ctx, src_srs_item, res_wkb))) {
                LOG_WARN("failed to write geometry to wkb", K(ret));
              } else {
                res.set_string(res_wkb);
                need_eval = false;
              }
            } else if (OB_FAIL(srs_guard.get_srs_item(dest_srid, dest_srs_item))) {
              LOG_WARN("failed to get dest srs", K(ret), K(dest_srid));
            } else if (OB_FAIL(dest_srs_item->get_proj4_param(&temp_allocator, dest_proj4_param))) {
              LOG_WARN("failed to get proj4 prams from dest srs", K(ret), K(dest_srid));
            }
          }
        } else {
          if (src_srid == 0) {
            ret = OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED, src_srid);
          } else {
            dest_proj4_param = datum2->get_string();
            if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *datum2,
                        expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), dest_proj4_param))) {
              LOG_WARN("fail to get real data.", K(ret), K(dest_proj4_param));
            }
          }
        }

        if (OB_SUCC(ret) && need_eval && src_srid != 0) {
          if (OB_FAIL(src_srs_item->get_proj4_param(&temp_allocator, src_proj4_param))) {
            LOG_WARN("failed to get proj4 prams from srs", K(ret), K(src_srid));
          }
        }
      }

      if (OB_SUCC(ret) && (param_num == 3)) {
        ObDatum *datum3 = NULL;
        src_proj4_param = datum2->get_string();
        src_srs_item = NULL;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *datum2,
                    expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), src_proj4_param))) {
          LOG_WARN("fail to get real data.", K(ret), K(src_proj4_param));
        } else if (OB_FAIL(expr.args_[2]->eval(ctx, datum3))) {
          LOG_WARN("failed to eval datum", K(ret));
        } else if (ob_is_integer_type(expr.args_[2]->datum_meta_.type_)) {
          dest_srid = datum3->get_int();
          if (datum3->get_int() < 0 || datum3->get_int() > UINT_MAX32) {
            ret = OB_OPERATE_OVERFLOW;
            LOG_WARN("srid input value out of range", K(ret), K(dest_srid));
          }
          dest_srid = datum3->get_int();
          if (OB_SUCC(ret)) {
            if (dest_srid == 0) {
              ret = OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED, dest_srid);
            } else if (OB_FAIL(srs_guard.get_srs_item(dest_srid, dest_srs_item))) {
              LOG_WARN("failed to get dest srs", K(ret), K(dest_srid));
            } else if (OB_FAIL(dest_srs_item->get_proj4_param(&temp_allocator, dest_proj4_param))) {
              LOG_WARN("failed to get proj4 prams from dest srs", K(ret), K(dest_srid));
            }
          }
        } else {
          dest_proj4_param = datum3->get_string();
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *datum3,
                      expr.args_[2]->datum_meta_, expr.args_[2]->obj_meta_.has_lob_header(), dest_proj4_param))) {
            LOG_WARN("fail to get real data.", K(ret), K(dest_proj4_param));
          }
        }
      }

      // eval by bg
      if (OB_SUCC(ret) && need_eval) {
        int correct_result;
        ObGeoEvalCtx correct_context(&temp_allocator, src_srs_item);
        ObGeoEvalCtx transform_context(&temp_allocator, dest_srs_item);
        ObString src_proj4;
        ObString dest_proj4;
        if (src_proj4_param.empty()) {
          ret = OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_ERR_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED, src_srid);
        } else if (dest_proj4_param.empty()) {
          ret = OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_ERR_TRANSFORM_TARGET_SRS_NOT_SUPPORTED, dest_srid);
        } else if (OB_FAIL(ob_write_string(temp_allocator, src_proj4_param, src_proj4, true))) {
          LOG_WARN("failed to transform proj4 to c style", K(ret), K(src_proj4));
        } else if (OB_FAIL(ob_write_string(temp_allocator, dest_proj4_param, dest_proj4, true))) {
          LOG_WARN("failed to transform proj4 to c style", K(ret), K(dest_proj4));
        } else if (OB_FAIL(ObGeoExprUtils::normalize_wkb(src_proj4, src_geo))) {
          LOG_WARN("failed to do normalize wkb", K(ret), K(src_proj4));
        } else if (OB_FAIL(correct_context.append_geo_arg(src_geo))) {
          LOG_WARN("failed to append geo arg to gis context", K(ret), K(correct_context.get_geo_count()));
        } else if (OB_FAIL(transform_context.append_geo_arg(src_geo))) {
          LOG_WARN("failed to append geo arg to gis context", K(ret), K(transform_context.get_geo_count()));
        } else if (OB_FAIL(transform_context.append_val_arg(&src_proj4))) {
          LOG_WARN("failed to append src_proj4_param to gis context", K(ret), K(transform_context.get_geo_count()));
        } else if (OB_FAIL(transform_context.append_val_arg(&dest_proj4))) {
          LOG_WARN("failed to append dest_proj4_param to gis context", K(ret), K(transform_context.get_geo_count()));
        } else if (OB_NOT_NULL(src_srs_item) && OB_FAIL(ObGeoFunc<ObGeoFuncType::Correct>::geo_func::eval(correct_context, correct_result))) {
          LOG_WARN("eval boost correct failed", K(ret));
        } else if (OB_FAIL(ObGeoFunc<ObGeoFuncType::Transform>::geo_func::eval(transform_context, dest_geo))) {
          LOG_WARN("eval boost transform failed", K(ret), K(src_proj4), K(dest_proj4), K(src_srid), K(dest_srid));
          ObGeoExprUtils::geo_func_error_handle(ret, N_PRIV_ST_TRANSFORM);
        } else if (dest_srs_item == NULL && OB_FAIL(ObGeoExprUtils::denormalize_wkb(dest_proj4, dest_geo))) {
          LOG_WARN("failed to do denormalize wkb", K(ret), K(dest_proj4));
        } else if ((OB_ISNULL(dest_srs_item) && dest_srid != 0) || OB_ISNULL(dest_geo)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected null srs_item or res_geo", K(ret), K(dest_srid), KP(dest_srs_item), KP(dest_geo));
        } else {
          ObString res_wkb;
          if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*dest_geo, expr, ctx, dest_srs_item, res_wkb, dest_srid))){
            LOG_WARN("failed to write geometry to wkb", K(ret));
          } else {
            res.set_string(res_wkb);
          }
        }
      }
    }
  }

  return ret;
}

int ObExprPrivSTTransform::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_priv_st_transform;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase