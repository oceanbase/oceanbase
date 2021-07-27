/**
 * Copyright 2014-2016 Alibaba Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 *
 * Authors:
 *     zimiao<kaizhan.dkz@antgroup.com>
 */
#define USING_LOG_PREFIX SQL_ENG
#include "sql/parser/ob_item_type.h"
#include "lib/oblog/ob_log.h"
#include "lib/number/ob_number_v2.h"
#include "sql/engine/expr/ob_expr_cot.h"
#include "sql/session/ob_sql_session_info.h"
#include <math.h>
namespace oceanbase
{
using namespace common;
using namespace common::number;
namespace sql
{
ObExprCot::ObExprCot(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_COT, N_COT, 1, NOT_ROW_DIMENSION)
{
}
ObExprCot::~ObExprCot()
{
}
int ObExprCot::calc_result_type1(ObExprResType &type,
                                 ObExprResType &radian,
                                 ObExprTypeCtx &type_ctx) const
{
  return calc_trig_function_result_type1(type, radian, type_ctx);
}
int ObExprCot::calc_result1(ObObj &result,
                            const ObObj &radian_obj,
                            ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (radian_obj.is_null()) {
    result.set_null();
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expr_ctx.calc_buf_ is NULL", K(ret));
  } else if (radian_obj.is_double()) {
    double arg = radian_obj.get_double();
    double tan_out = tan(arg);
    // to check tan(arg) is small enough to be zero
    if (0.0 == tan_out) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tan(x) is zero", K(ret));
    } else {
      double res = 1.0/tan_out;
      if (!std::isfinite(res)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tan(x) is like zero", K(ret));
      } else {
        result.set_double(res);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}
} /* sql */
} /* oceanbase */