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
 * This file contains implementation for spatial_mbr expr.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_spatial_mbr.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/geo/ob_s2adapter.h"
#include "observer/omt/ob_tenant_srs.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprSpatialMbr::ObExprSpatialMbr(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_SPATIAL_MBR,
                         N_SPATIAL_MBR,
                         1,
                         NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION)
{

}

ObExprSpatialMbr::~ObExprSpatialMbr()
{

}

int ObExprSpatialMbr::calc_result_type1(ObExprResType &type,
                                        ObExprResType &type1,
                                        common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_length(OB_DEFAULT_MBR_SIZE);
  type.set_collation_type(CS_TYPE_BINARY);
  type.set_collation_level(CS_LEVEL_IMPLICIT);
  return ret;
}

// for old sql engine
int ObExprSpatialMbr::calc_result1(common::ObObj &result,
                                   const common::ObObj &obj,
                                   common::ObExprCtx &expr_ctx) const
{
  UNUSED(obj);
  UNUSED(expr_ctx);
  result.set_null();
  return OB_SUCCESS;
}

int ObExprSpatialMbr::eval_spatial_mbr(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &res)
{
  UNUSED(expr);
  UNUSED(ctx);
  res.set_null();
  return OB_SUCCESS;
}

int ObExprSpatialMbr::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_spatial_mbr;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase