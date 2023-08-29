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
 * This file contains implementation for st_srid.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_st_srid.h"
#include "lib/geo/ob_geo_common.h"
#include "lib/geo/ob_geo_func_common.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprSTSRID::ObExprSTSRID(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ST_SRID, N_ST_SRID, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSTSRID::ObExprSTSRID(ObIAllocator &alloc,
                           ObExprOperatorType type,
                           const char *name,
                           int32_t param_num,
                           int32_t dimension) : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprSTSRID::~ObExprSTSRID()
{
}

int ObExprSTSRID::calc_result_typeN(ObExprResType& type,
                                    ObExprResType* types_stack,
                                    int64_t param_num,
                                    ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(param_num > 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    if (ob_is_null(types_stack[0].get_type())) {
    } else if (ob_is_numeric_type(types_stack[0].get_type())) {
      types_stack[0].set_calc_type(ObLongTextType);
    } else if (!ob_is_geometry(types_stack[0].get_type()) && !ob_is_string_type(types_stack[0].get_type())) {
      ret = OB_ERR_GIS_INVALID_DATA;
      LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_name());
    } else {
      type.set_uint32();
    }
    if (OB_SUCC(ret) && param_num > 1) {
      types_stack[1].set_calc_type(ObIntType);
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
      type.set_geometry();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
    }
  }

  return ret;
}

int ObExprSTSRID::eval_st_srid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_st_srid_common(expr, ctx, res, N_ST_SRID);
}

int ObExprSTSRID::eval_st_srid_common(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res, const char *func_name)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObDatum *datum = NULL;
  int num_args = expr.arg_cnt_;
  bool is_null_result = false;
  ObGeoSrid srid = 0;
  ObString wkb;
  ObString res_wkb;
  const ObSrsItem *srs = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  omt::ObSrsCacheGuard srs_guard;
  ObGeometry *geo = NULL;
  bool is_geog = false;

  // get srid
  if (num_args > 1) {
    if (expr.args_[1]->is_boolean_ && T_FUN_SYS_PRIV_ST_SETSRID == expr.type_) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid type", K(ret));
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, datum))) {
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
      } else if (OB_FAIL(srs_guard.get_srs_item(srid, srs))) {
        LOG_WARN("failed to get srs item", K(ret));
      } else if (OB_ISNULL(srs)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null srs item", K(ret));
      }
    }
  }

  // get geometry
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
      LOG_WARN("failed to eval first argument", K(ret));
    } else if (datum->is_null()) {
      is_null_result = true;
    } else if (!is_null_result) { // srid might be null, fix 42538503
      wkb = datum->get_string();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
                expr.args_[0]->datum_meta_, expr.args_[0]->obj_meta_.has_lob_header(), wkb))) {
        LOG_WARN("fail to get real string data", K(ret), K(wkb));
      } else if (num_args == 1) {
        if (OB_FAIL(ObGeoExprUtils::get_srs_item(ctx, srs_guard, wkb, srs, true, func_name))) {
          LOG_WARN("fail to get srs item", K(ret), K(wkb));
          if (OB_ERR_SRS_NOT_FOUND == ret) {
            ret = OB_SUCCESS; // adapt mysql, treat unknown srid as cartesian
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(ObGeoExprUtils::build_geometry(tmp_allocator, wkb, geo, srs, func_name, false, false, false))) {
          LOG_WARN("failed to parse geometry from wkb", K(ret));
        } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(wkb, srid))) {
          LOG_WARN("failed to get srid from wkb", K(ret));
        } else if (0 != srid && srs == NULL) {
          LOG_USER_WARN(OB_ERR_WARN_SRS_NOT_FOUND, srid);
          LOG_WARN("srs not found");
        }
      } else {
        if (OB_FAIL(ObGeoExprUtils::build_geometry(tmp_allocator, wkb, geo, srs, func_name, false, false, false))) {
          LOG_WARN("fail to create geo", K(ret), K(wkb));
        } else if (OB_FAIL(ObGeoExprUtils::geo_to_wkb(*geo, expr, ctx, srs, res_wkb))) {
          LOG_WARN("failed to write geometry to wkb", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_result) {
    res.set_null();
  } else if (num_args == 1) {
    res.set_uint32(srid);
  } else if (OB_ISNULL(geo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null geometry", K(ret));
  } else {
    res.set_string(res_wkb);
  }

  return ret;
}

int ObExprSTSRID::cg_expr(ObExprCGCtx &expr_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_st_srid;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase