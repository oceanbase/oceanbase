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
 * This file contains implementation for _st_setsrid.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_priv_st_setsrid.h"
#include "lib/geo/ob_geo_common.h"
#include "lib/geo/ob_geo_func_common.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_geo.h"
#include "observer/omt/ob_tenant_srs.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/object/ob_obj_cast_util.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprPrivSTSetSRID::ObExprPrivSTSetSRID(ObIAllocator &alloc)
    : ObExprSTSRID(alloc, T_FUN_SYS_PRIV_ST_SETSRID, N_PRIV_ST_SETSRID, 2, NOT_ROW_DIMENSION)
{
}

ObExprPrivSTSetSRID::~ObExprPrivSTSetSRID()
{
}

int ObExprPrivSTSetSRID::calc_result_type2(ObExprResType &type,
                                           ObExprResType &type1,
                                           ObExprResType &type2,
                                           common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  ObObjType type_geom = type1.get_type();
  ObObjType type_srid = type2.get_type();

  if (!ob_is_geometry(type_geom)) {
    ret = OB_ERR_GIS_INVALID_DATA;
    LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, get_name());
  } else {
    const ObSQLSessionInfo *session =
    dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast basic session to sql session info failed", K(ret));
    } else if (!ob_is_integer_type(type_srid)
        && !ob_is_string_type(type_srid)
        && !ob_is_null(type_srid)) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid input type srid", K(ret), K(type_srid));
    } else if (ob_is_string_type(type_srid)) {
      type2.set_calc_type(ObIntType);
    }
  }

  if (OB_SUCC(ret)) {
      ObCastMode cast_mode = type_ctx.get_cast_mode();
      cast_mode &= ~CM_WARN_ON_FAIL; // make cast return error when fail
      cast_mode |= CM_STRING_INTEGER_TRUNC; // make cast check range when string to int
      type_ctx.set_cast_mode(cast_mode); // cast mode only do work in new sql engine cast frame.
      type.set_geometry();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObGeometryType]).get_length());
  }

  return ret;
}

int ObExprPrivSTSetSRID::eval_priv_st_setsrid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_st_srid_common(expr, ctx, res, N_PRIV_ST_SETSRID);
}

int ObExprPrivSTSetSRID::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  UNUSEDx(expr_cg_ctx, raw_expr);
  rt_expr.eval_func_ = eval_priv_st_setsrid;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase