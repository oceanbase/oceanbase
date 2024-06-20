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
 * This file contains implementation for _st_geogfromtext.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_geogfromtext.h"
#include "lib/geo/ob_geo_common.h"
#include "lib/geo/ob_wkt_parser.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "lib/geo/ob_geo_reverse_coordinate_visitor.h"
#include "lib/geo/ob_geo_func_common.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTGeogFromText::ObExprPrivSTGeogFromText(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_PRIV_ST_GEOGFROMTEXT, N_PRIV_ST_GEOGFROMTEXT, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprPrivSTGeogFromText::ObExprPrivSTGeogFromText(ObIAllocator &alloc,
                                                   ObExprOperatorType type,
                                                   const char *name,
                                                   int32_t param_num,
                                                   int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, NOT_VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprPrivSTGeogFromText::~ObExprPrivSTGeogFromText()
{
}

int ObExprPrivSTGeogFromText::calc_result_type1(ObExprResType &type,
                                                ObExprResType &type1,
                                                common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (ob_is_null(type1.get_type())) {
  } else if (!ob_is_string_type(type1.get_type())
              || ObCharset::is_cs_nonascii(type1.get_collation_type())) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_name());
  }
  type.set_geometry();
  type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  return ret;
}

int ObExprPrivSTGeogFromText::eval_priv_st_geogfromtext(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_priv_st_geogfromtext_common(expr, ctx, res, N_PRIV_ST_GEOGFROMTEXT);
}

int ObExprPrivSTGeogFromText::eval_priv_st_geogfromtext_common(const ObExpr &expr,
                                                               ObEvalCtx &ctx,
                                                               ObDatum &res,
                                                               const char *func_name)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObDatum *datum = NULL;

  // get wkt
  if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("eval wkt arg failed", K(ret));
  } else if(datum->is_null()){
    res.set_null();
  } else {
    ObGeoSrid srid = 0;
    const ObSrsItem *srs_item = NULL;
    omt::ObSrsCacheGuard srs_guard;
    ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    ObGeometry *geo = NULL;
    bool is_geog = false;
    ObString wkt = datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkt))) {
      LOG_WARN("fail to get real data.", K(ret), K(wkt));
    } else if (OB_NOT_NULL(wkt.find(';'))) {
      ObString srid_str = wkt.split_on(';');
      if (OB_FAIL(ObGeoExprUtils::parse_srid(srid_str, srid))) {
        LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
        LOG_WARN("parse_srid failed", K(ret), K(srid_str));
      } else if (srid == 0) {
        srid = OB_GEO_DEFAULT_GEOGRAPHY_SRID;
      }
    } else {
      srid = OB_GEO_DEFAULT_GEOGRAPHY_SRID;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
      LOG_WARN("failed to get srs guard", K(ret));
    } else if (OB_FAIL(srs_guard.get_srs_item(srid, srs_item))) {
      LOG_WARN("failed to get srs item", K(ret));
    } else if (OB_ISNULL(srs_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null srs item", K(ret));
    } else {
      is_geog = srs_item->is_geographical_srs();
    }

    if (OB_FAIL(ret)) {
    } else if (!is_geog) {
      ret = OB_ERR_SRS_NOT_GEOGRAPHIC;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
      LOG_WARN("Only lon/lat coordinate systems are supported in geography.", K(ret), K(srid));
    } else if (OB_FAIL(ObWktParser::parse_wkt(tmp_allocator, wkt, geo, true, is_geog))) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, func_name);
      LOG_WARN("failed to parse wkt", K(ret));
    } else if (OB_ISNULL(geo)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null geo after parse_wkt", K(ret), K(wkt));
    } else if (OB_FAIL(ObGeoExprUtils::correct_coordinate_range(srs_item, geo, func_name))) {
      LOG_WARN("check geo coordinate range failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      ObString res_wkb;
      if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*geo, expr, ctx, srs_item, res_wkb))) {
        LOG_WARN("failed to write geometry to wkb", K(ret));
      } else {
        res.set_string(res_wkb);
      }
    }
  }

  return ret;
}

int ObExprPrivSTGeogFromText::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                      const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_priv_st_geogfromtext;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase