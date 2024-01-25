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
 * This file contains implementation for st_geomfromtext.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_geomfromtext.h"
#include "lib/geo/ob_wkt_parser.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "lib/geo/ob_geo_coordinate_range_visitor.h"
#include "lib/geo/ob_geo_reverse_coordinate_visitor.h"
#include "lib/geo/ob_geo_func_common.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprSTGeomFromText::ObExprSTGeomFromText(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_GEOMFROMTEXT, N_ST_GEOMFROMTEXT, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTGeomFromText::ObExprSTGeomFromText(ObIAllocator &alloc,
                                           ObExprOperatorType type,
                                           const char *name,
                                           int32_t param_num,
                                           int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprSTGeomFromText::~ObExprSTGeomFromText()
{
}

int ObExprSTGeomFromText::calc_result_typeN(ObExprResType& type,
                                            ObExprResType* types_stack,
                                            int64_t param_num,
                                            ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    for (uint8_t i = 0; i < param_num && OB_SUCC(ret); i++) {
      if (i == 0 || i == 2) {
        if (ob_is_null(types_stack[i].get_type())) {
        } else if (!ob_is_string_type(types_stack[i].get_type())
                   || ObCharset::is_cs_nonascii(types_stack[i].get_collation_type())) {
          types_stack[i].set_calc_type(common::ObVarcharType);
          types_stack[i].set_calc_collation_type(CS_TYPE_BINARY);
        }
      }
      // srid
      if (i == 1) {
        types_stack[i].set_calc_type(ObIntType);
        type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
      }
    }
    type.set_geometry();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  }

  return ret;
}

int ObExprSTGeomFromText::eval_st_geomfromtext(const ObExpr &expr,
                                               ObEvalCtx &ctx,
                                               ObDatum &res)
{
  return eval_st_geomfromtext_common(expr, ctx, res, N_ST_GEOMFROMTEXT);
}

int ObExprSTGeomFromText::eval_st_geomfromtext_common(const ObExpr &expr,
                                                      ObEvalCtx &ctx,
                                                      ObDatum &res,
                                                      const char *func_name)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObDatum *datum = NULL;
  int num_args = expr.arg_cnt_;
  bool is_null_result = false;
  uint32_t srid = 0;
  ObGeoAxisOrder axis_order = ObGeoAxisOrder::INVALID;
  ObString wkt;
  const ObSrsItem *srs_item = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  omt::ObSrsCacheGuard srs_guard;
  ObGeometry *geo = NULL;
  bool is_lat_long = false;
  bool is_geog = false;
  bool need_reverse = false;
  bool is_3d_geo = false;

  // get wkt
  if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("failed to eval first argument", K(ret));
  } else if (datum->is_null()) {
    is_null_result = true;
  } else {
    wkt = datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
        expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkt))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkt));
    }
  }
  // get srid
  if (!is_null_result && OB_SUCC(ret) && num_args > 1) {
    if (OB_FAIL(expr.args_[1]->eval(ctx, datum))) {
      LOG_WARN("failed to eval second argument", K(ret));
    } else if (datum->is_null()) {
      is_null_result = true;
    } else if (datum->get_int() < 0 || datum->get_int() > UINT_MAX32) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "SRID", func_name);
      LOG_WARN("srid input value out of range", K(ret), K(datum->get_int()));
    } else if (0 != (srid = datum->get_uint32())) {
      if (OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
        LOG_WARN("failed to get srs guard", K(ret));
      } else if (OB_FAIL(srs_guard.get_srs_item(srid, srs_item))) {
        LOG_WARN("failed to get srs item", K(ret));
      } else if (OB_ISNULL(srs_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null srs item", K(ret));
      } else {
        is_geog = srs_item->is_geographical_srs();
        is_lat_long = srs_item->is_lat_long_order();
        need_reverse = is_geog && is_lat_long;
      }
    }
  }
  // get axis_order
  if (!is_null_result && OB_SUCC(ret) && num_args > 2 ) {
    ObString axis_str;
    if (OB_FAIL(expr.args_[2]->eval(ctx, datum))) {
      LOG_WARN("failed to eval third argument", K(ret));
    } else if (datum->is_null()){
      is_null_result = true;
    } else if (FALSE_IT(axis_str = datum->get_string())) {
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
              expr.args_[2]->datum_meta_, expr.args_[2]->obj_meta_.has_lob_header(), axis_str))) {
      LOG_WARN("fail to get real string data", K(ret), K(axis_str));
    } else if (OB_FAIL(ObGeoExprUtils::parse_axis_order(axis_str, func_name, axis_order))) {
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
    if (OB_FAIL(ObWktParser::parse_wkt(tmp_allocator, wkt, geo, true, is_geog))) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
      LOG_WARN("failed to parse wkt", K(ret));
    } else if (OB_ISNULL(geo)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null geo after parse_wkt", K(ret), K(wkt));
    } else {
      is_3d_geo = ObGeoTypeUtil::is_3d_geo_type(geo->type());
      if (is_geog && need_reverse && OB_FAIL(ObGeoExprUtils::reverse_coordinate(geo, func_name))) {
        LOG_WARN("failed to reverse geometry coordinate", K(ret));
      }
      if (is_geog && OB_SUCC(ret)) {
        if (OB_FAIL(ObGeoExprUtils::check_coordinate_range(srs_item, geo, func_name))) {
          LOG_WARN("check geo coordinate range failed", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_result) {
    res.set_null();
  } else if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null geometry", K(ret));
  } else {
    ObString res_wkb;
    if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*geo, expr, ctx, srs_item, res_wkb))) {
      LOG_WARN("failed to write geometry to wkb", K(ret));
    } else {
      res.set_string(res_wkb);
    }
  }

  return ret;
}

int ObExprSTGeomFromText::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_geomfromtext;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
