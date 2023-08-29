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
 * This file contains implementation for st_geomfromewkt.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_geomfromewkt.h"
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
ObExprPrivSTGeomFromEwkt::ObExprPrivSTGeomFromEwkt(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_GEOMFROMEWKT, N_PRIV_ST_GEOMFROMEWKT, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprPrivSTGeomFromEwkt::~ObExprPrivSTGeomFromEwkt()
{
}

int ObExprPrivSTGeomFromEwkt::calc_result_type1(ObExprResType &type,
                                            ObExprResType &type1,
                                            common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (ob_is_null(type1.get_type())) {
  } else if (!ob_is_string_type(type1.get_type())
              || ObCharset::is_cs_nonascii(type1.get_collation_type())) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_GEOMFROMEWKT);
  }
  type.set_geometry();
  type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  return ret;
}

// ewkt is always long-lat
int ObExprPrivSTGeomFromEwkt::eval_st_geomfromewkt(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObDatum *datum = NULL;
  bool is_null_result = false;
  uint32_t srid = 0;
  ObString wkt;
  const ObSrsItem *srs_item = NULL;
  omt::ObSrsCacheGuard srs_guard;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObGeometry *geo = NULL;
  bool is_geog = false;

  // get wkt
  if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("eval wkt arg failed", K(ret));
  } else if(datum->is_null()){
    is_null_result = true;
  } else {
    wkt = datum->get_string();
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
        expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkt))) {
      LOG_WARN("fail to get real string data", K(ret), K(wkt));
    }
  }
  // get srid
  if (!is_null_result && OB_SUCC(ret) && OB_NOT_NULL(wkt.find(';'))) {
    ObString srid_str = wkt.split_on(';');
    if (OB_FAIL(ObGeoExprUtils::parse_srid(srid_str, srid))) {
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_GEOMFROMEWKT);
      LOG_WARN("parse_srid  failed", K(ret), K(srid_str));
    } else if (srid < 0 || srid > UINT_MAX32) {
      ret = OB_OPERATE_OVERFLOW;
      LOG_WARN("srid input value out of range", K(ret), K(datum->get_int()));
    } else if (0 != srid) {
      if (OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
        LOG_WARN("failed to get srs guard", K(ret));
      } else if (OB_FAIL(srs_guard.get_srs_item(srid, srs_item))) {
        LOG_WARN("failed to get srs item", K(ret));
      } else if (OB_ISNULL(srs_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null srs item", K(ret));
      } else {
        is_geog = srs_item->is_geographical_srs();
      }
    }
  }

  if (OB_SUCC(ret) && !is_null_result) {
    if (OB_FAIL(ObWktParser::parse_wkt(tmp_allocator, wkt, geo, true, is_geog))) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_PRIV_ST_GEOMFROMEWKT);
      LOG_WARN("failed to parse wkt", K(ret));
    } else if (OB_ISNULL(geo)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null geo after parse_wkt", K(ret), K(wkt));
    } else {
      if (is_geog && OB_SUCC(ret)) {
        if (OB_FAIL(ObGeoExprUtils::check_coordinate_range(srs_item, geo, N_PRIV_ST_GEOMFROMEWKT))) {
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

int ObExprPrivSTGeomFromEwkt::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                  const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_st_geomfromewkt;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase