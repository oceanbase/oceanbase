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
 * This file contains implementation for st_astext.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_astext.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo_reverse_coordinate_visitor.h"
#include "lib/geo/ob_geo_to_wkt_visitor.h"
#include "lib/geo/ob_geo_func_common.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprSTAsText::ObExprSTAsText(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_ASTEXT, N_ST_ASTEXT, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTAsText::ObExprSTAsText(ObIAllocator &alloc,
                               ObExprOperatorType type,
                               const char *name,
                               int32_t param_num,
                               int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension)
{
}

int ObExprSTAsText::calc_result_typeN(ObExprResType& type,
                                      ObExprResType* types_stack,
                                      int64_t param_num,
                                      ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 2)) {
    ObString fun_name(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, fun_name.length(), fun_name.ptr());
  } else {
    if (1 == param_num) {
      ObObjType data_type = types_stack[0].get_type();
      if (ob_is_geometry(data_type) || ob_is_null(data_type)) {
        // do nothing
      } else {
        types_stack[0].set_calc_type(ObLongTextType);
        types_stack[0].set_calc_collation_type(CS_TYPE_BINARY);
        types_stack[0].set_calc_collation_level(CS_LEVEL_IMPLICIT);
      }
    }

    if (2 == param_num) {
      ObObjType option_type = types_stack[1].get_type();
      if (ob_is_string_type(option_type) || ob_is_null(option_type)) {
        // do nothing
      } else {
        types_stack[1].set_calc_type(ObVarcharType);
        types_stack[1].set_calc_collation_type(types_stack[1].get_collation_type());
        types_stack[1].set_calc_collation_level(types_stack[1].get_collation_level());
      }
    }

    if (OB_SUCC(ret)) {
      type.set_type(ObLongTextType);
      type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);
    }
  }
  return ret;
}

int ObExprSTAsText::eval_st_astext_common(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObDatum &res,
                                          const char *func_name)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  int num_args = expr.arg_cnt_;
  bool is_null_result = false;
  ObString res_wkt;
  omt::ObSrsCacheGuard srs_guard;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  const ObSrsItem *srs = NULL;
  ObGeometry *geo = NULL;
  bool is_geog = false;
  bool need_reverse = false;
  ObDatum *gis_datum = NULL;
  ObString wkb;
  // get geo
  if (OB_FAIL(expr.args_[0]->eval(ctx, gis_datum))) {
    LOG_WARN("eval geo args failed", K(ret));
  } else if (gis_datum->is_null()) {
    is_null_result = true;
  } else if (FALSE_IT(wkb = gis_datum->get_string())) {
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *gis_datum,
             expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
    LOG_WARN("fail to get real string data", K(ret), K(wkb));
  } else if (OB_FAIL(ObGeoExprUtils::construct_geometry(tmp_allocator,
      wkb, srs_guard, srs, geo, func_name))) {
    LOG_WARN("fail to create geo", K(ret), K(wkb));
  } else if (OB_NOT_NULL(srs)){
    is_geog = srs->is_geographical_srs();
    need_reverse = is_geog && (srs->is_lat_long_order());
  }

  // get axis_order
  if (!is_null_result && OB_SUCC(ret) && num_args > 1 ) {
    ObGeoAxisOrder axis_order = ObGeoAxisOrder::INVALID;
    ObDatum *datum = NULL;
    ObString dstr;
    if (OB_FAIL(expr.args_[1]->eval(ctx, datum))) {
      LOG_WARN("eval axis_order axis_order failed", K(ret));
    } else if (datum->is_null()){
      is_null_result = true;
    } else if (!ob_is_string_type(expr.args_[1]->datum_meta_.type_) ||
               ObCharset::is_cs_nonascii(expr.args_[1]->datum_meta_.cs_type_)) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
    } else if (FALSE_IT(dstr = datum->get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
              expr.args_[1]->datum_meta_, expr.args_[1]->obj_meta_.has_lob_header(), dstr))) {
      LOG_WARN("fail to get real string data", K(ret), K(dstr));
    } else if (OB_FAIL(ObGeoExprUtils::parse_axis_order(dstr, func_name, axis_order))) {
      LOG_WARN("failed to parse axis order option string", K(ret));
    } else {
      switch (axis_order) {
        case ObGeoAxisOrder::LONG_LAT: {
          need_reverse = false;
          break;
        }
        case ObGeoAxisOrder::LAT_LONG: {
          need_reverse = true;
          break;
        }
        case ObGeoAxisOrder::SRID_DEFINED: {
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected axis order parse result", K(ret));
          break;
        }
      }
    }
  }

  if (!is_null_result && OB_SUCC(ret)) {
    if (OB_ISNULL(geo)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null geo", K(ret));
    } else {
      if (is_geog && need_reverse) {
        ObGeoReverseCoordinateVisitor rcoord_visitor;
        if (OB_FAIL(geo->do_visit(rcoord_visitor))) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
          LOG_WARN("failed to reverse geometry coordinate", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        ObGeoToWktVisitor wkt_visitor(&tmp_allocator);
        if (OB_FAIL(geo->do_visit(wkt_visitor))) {
          ret = OB_ERR_GIS_INVALID_DATA;
          LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
          LOG_WARN("failed to transform geo to wkt", K(ret));
        } else {
          wkt_visitor.get_wkt(res_wkt);
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_result) {
    res.set_null();
  } else if (OB_FAIL(ObGeoExprUtils::pack_geo_res(expr, ctx, res, res_wkt))) {
    LOG_WARN("fail to pack geo res", K(ret));
  }

  return ret;
}

int ObExprSTAsText::eval_st_astext(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_st_astext_common(expr, ctx, res, N_ST_ASTEXT);
}

int ObExprSTAsWkt::eval_st_astext(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_st_astext_common(expr, ctx, res, N_ST_ASWKT);
}

int ObExprSTAsText::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_astext;
  return OB_SUCCESS;
}

int ObExprSTAsWkt::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_astext;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
