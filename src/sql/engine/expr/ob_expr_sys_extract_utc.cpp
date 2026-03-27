/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_sys_extract_utc.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprSysExtractUtc::ObExprSysExtractUtc(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SYS_EXTRACT_UTC, N_SYS_EXTRACT_UTC, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprSysExtractUtc::calc_result_type1(ObExprResType &type,
                                           ObExprResType &input,
                                           common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!input.get_obj_meta().is_otimestamp_type()
      && !input.is_null())) {
    ret = common::OB_ERR_INVALID_TYPE_FOR_ARGUMENT;

    LOG_WARN("invalid type", K(input.get_type()));
  } else {
    type.set_timestamp_nano();
    type.set_scale(input.get_scale());
    input.set_calc_type(ObTimestampTZType);
  }
  return ret;
}

int ObExprSysExtractUtc::calc_sys_extract_utc(const ObExpr &expr, ObEvalCtx &ctx,
                                              ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *in = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, in))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (in->is_null()) {
    res.set_null();
  } else {
    ObOTimestampData in_timestamp = in->get_otimestamp_tz();
    in_timestamp.time_ctx_.tz_desc_ = 0;
    in_timestamp.time_ctx_.store_tz_id_ = 0;
    res.set_otimestamp_tiny(in_timestamp);
  }
  return ret;
}

int ObExprSysExtractUtc::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_sys_extract_utc;
  return ret;
}
}
}
