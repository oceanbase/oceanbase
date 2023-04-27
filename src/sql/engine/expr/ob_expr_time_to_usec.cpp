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
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_time_to_usec.h"
#include "lib/time/ob_time_utility.h"
#include "lib/ob_name_def.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprTimeToUsec::ObExprTimeToUsec(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TIME_TO_USEC, N_TIME_TO_USEC, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprTimeToUsec::~ObExprTimeToUsec()
{
}


int ObExprTimeToUsec::calc_result_type1(ObExprResType &type,
                                        ObExprResType &date,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!date.is_varchar()
      && !date.is_temporal_type()
      && !date.is_null())) {
    ret = common::OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC;

    LOG_WARN("invalid type", K(date.get_type()));
  } else {
    type.set_int();
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
    type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
    //set calc type
    date.set_calc_type(ObTimestampType);
  }
  return ret;
}

int calc_time_to_usec_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                  ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (arg_datum->is_null()) {
    res_datum.set_null();
  } else {
    res_datum.set_int(arg_datum->get_timestamp());
  }
  return ret;
}

int ObExprTimeToUsec::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_time_to_usec_expr;
  return ret;
}

}
}
