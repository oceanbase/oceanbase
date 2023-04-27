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

#include "sql/engine/expr/ob_expr_usec_to_time.h"
#include "lib/ob_name_def.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprUsecToTime::ObExprUsecToTime(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_USEC_TO_TIME, N_USEC_TO_TIME, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUsecToTime::~ObExprUsecToTime()
{
}

int ObExprUsecToTime::calc_result_type1(ObExprResType &type,
                                        ObExprResType &usec,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(common::ObIntTC == type.get_type_class() && common::ObNullType != usec.get_type())) {
    ret = common::OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME;

  } else {
    type.set_timestamp();
    type.set_scale(common::MAX_SCALE_FOR_TEMPORAL); //和时间值有关，取最大值
    //set calc type
    usec.set_calc_type(ObIntType);
  }
  return ret;
}

int calc_usec_to_time_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *usec_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, usec_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (usec_datum->is_null()) {
    res_datum.set_null();
  } else {
    int64_t value = usec_datum->get_int();
    int32_t offset = 0;
    const ObTimeZoneInfo* tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());
    if (NULL != tz_info && ObTimeConverter::ZERO_DATETIME != value) {
      if (OB_FAIL(tz_info->get_timezone_offset(USEC_TO_SEC(value), offset))) {
        LOG_WARN("failed to get offset between utc and local", K(ret));
      } else {
        value += SEC_TO_USEC(offset);
      }
    }
    if (OB_SUCC(ret)) {
      if (!ObTimeConverter::is_valid_datetime(value)) {
        ret = OB_DATETIME_FUNCTION_OVERFLOW;
        value -= SEC_TO_USEC(offset);
        LOG_WARN("datetime overflow", K(ret), K(value));
      } else {
        value -= SEC_TO_USEC(offset);
        res_datum.set_int(value);
      }
    }
  }
  return ret;
}

int ObExprUsecToTime::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_usec_to_time_expr;
  return ret;
}
}
}
