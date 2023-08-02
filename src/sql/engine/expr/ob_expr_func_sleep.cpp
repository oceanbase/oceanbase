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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_func_sleep.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprSleep::ObExprSleep(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SLEEP, N_SYS_SLEEP, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSleep::~ObExprSleep()
{
}

int ObExprSleep::calc_result_type1(ObExprResType &type,
                                   ObExprResType &param,
                                   ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_int();
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
  CK(OB_NOT_NULL(type_ctx.get_session()));
  if (OB_SUCC(ret)) {
    param.set_calc_type(ObNumberType);
  }
  return ret;
}

int ObExprSleep::sleep(int64_t usec)
{
  int ret = OB_SUCCESS;
  //do not worry about the overflow of this addition
  //if it does happen, the usec_rem will be 0 inside the while loop
  //and the loop will be break
  int64_t dead_line = ObTimeUtility::current_time() + usec;
  int64_t usec_rem = usec;
  useconds_t usec_req = static_cast<useconds_t>(MIN(CHECK_INTERVAL_IN_US, usec_rem));
  ObWaitEventGuard wait_guard(ObWaitEventIds::DEFAULT_SLEEP, 0, usec);
  while(usec_req > 0) {
    ob_usleep(usec_req);
    if (OB_FAIL(THIS_WORKER.check_status())) {
      break;
    } else {
      int64_t current_time = ObTimeUtility::current_time();
      usec_rem = current_time < dead_line ? dead_line - current_time : 0;
      usec_req = static_cast<useconds_t>(MIN(CHECK_INTERVAL_IN_US, usec_rem));
    }
  }//end while
  return ret;
}

int ObExprSleep::get_usec(const number::ObNumber &nmb, int64_t &value, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  value = 0;
  if (OB_SUCC(ret)) {
    number::ObNumber other;
    number::ObNumber res;
    //use nsecond to check range
    const int64_t nsecs_per_sec = NSECS_PER_SEC;
    uint64_t tmp = 0;
    number::ObNumber tmp_nmb;
    if (OB_FAIL(other.from(nsecs_per_sec, alloc))) {
      LOG_WARN("copy nmb failed", K(ret), K(nmb));
    } else if (OB_FAIL(tmp_nmb.from(nmb, alloc))) {
      LOG_WARN("copy nmb failed", K(ret), K(nmb));
    } else if (OB_FAIL(tmp_nmb.round(SCALE_OF_SECOND))) {
      LOG_WARN("round nmb failed", K(ret), K(tmp_nmb));
    } else if (OB_FAIL(tmp_nmb.mul(other, res, alloc))) {
      LOG_WARN("mul op failed", K(ret), K(other), K(tmp_nmb));
    } else if (!res.is_valid_uint64(tmp)) { //based on the behaviour of mysql.
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("out of range", K(ret), K(res), K(tmp));
    } else {
      //use usecond to calc
      value = tmp / NSECS_PER_USEC;
    }
  }
  return ret;
}

int ObExprSleep::eval_sleep(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *sec = NULL;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  ObIAllocator &calc_alloc = alloc_guard.get_allocator();
  int64_t usec = 0;
  if (OB_FAIL(expr.eval_param_value(ctx, sec))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (sec->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "sleep");
    LOG_WARN("invalid arguments to sleep");
  } else if (OB_FAIL(get_usec(number::ObNumber(sec->get_number()), usec, calc_alloc))) {
    ret = OB_SUCCESS;
  } else {
    ret = sleep(usec);
  }
  res.set_int(0);
  return ret;
}

int ObExprSleep::cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(raw_expr);
  CK(1 == rt_expr.arg_cnt_);
  OX(rt_expr.eval_func_ = eval_sleep);
  return ret;
}

} //namespace sql
} //namespace oceanbase
