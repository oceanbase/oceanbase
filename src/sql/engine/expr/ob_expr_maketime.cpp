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

#include <math.h>
#include "lib/timezone/ob_time_convert.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_maketime.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprMakeTime::ObExprMakeTime(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_MAKETIME, N_MAKETIME, 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprMakeTime::~ObExprMakeTime()
{
}

int ObExprMakeTime::fetch_second_quotient(int64_t &quotient, number::ObNumber &sec)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sec.extract_valid_int64_with_trunc(quotient))) {
    LOG_WARN("fail to get second quotient", K(ret));
  }
  return ret;
}

int ObExprMakeTime::fetch_second_remainder(const int64_t quotient, int64_t &remainder,
                                              ObIAllocator &alloc, number::ObNumber &sec)
{
  int ret = OB_SUCCESS;
  number::ObNumber second_num_quotient;
  number::ObNumber second_num_remainder;
  number::ObNumber second_num_remainder_transform;
  number::ObNumber const_1M;
  OZ(second_num_quotient.from(quotient, alloc));
  OZ(sec.sub(second_num_quotient, second_num_remainder, alloc));
  // According to Mysql User Manual(from 5.x to 8.0), the second scale is 6(range 0 - 999999).
  // https://dev.mysql.com/doc/refman/8.0/en/time.html
  OZ(const_1M.from((int64_t) 1000000, alloc));
  // Transformation example: 0.1234564 * 1000,000 => 123456.4
  OZ(second_num_remainder.mul(const_1M, second_num_remainder_transform, alloc));
  OZ(second_num_remainder_transform.round(common::DEFAULT_SCALE_FOR_INTEGER));
  OZ(second_num_remainder_transform.extract_valid_int64_with_trunc(remainder));
  return ret;
}

int ObExprMakeTime::eval_batch_maketime(const ObExpr &expr, ObEvalCtx &ctx,
                          const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("eval_batch_maketime start: batch mode", K(batch_size));
  ObDatumVector results  = expr.locate_expr_datumvector(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDatumVector input_hours;
  ObDatumVector input_mins;
  ObDatumVector input_secs;
  if (OB_FAIL(expr.eval_batch_param_value(ctx, skip, batch_size, input_hours,
                                          input_mins, input_secs))) {
    LOG_WARN("unexpected input value", K(ret));
    ret = OB_ERR_UNEXPECTED;
  } else if (input_hours.at(0) == nullptr || input_mins.at(0) == nullptr
             || input_secs.at(0) == nullptr) {
    LOG_WARN("unexpected input value", K(input_hours.at(0)),
             K(input_mins.at(0)), K(input_secs.at(0)));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObTime ob_time(DT_TYPE_TIME);
    for(auto i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      ObDatum * hour = input_hours.at(i);
      ObDatum * min  = input_mins.at(i);
      ObDatum * sec  = input_secs.at(i);
      if (OB_UNLIKELY(hour->is_null()) ||
          OB_UNLIKELY(min->is_null()) ||
          OB_UNLIKELY(sec->is_null())) {
        results.datums_[i].set_null();
      } else {
        int32_t h = hour->get_int32();
        int32_t m = min->get_int32();
        number::ObNumber s(sec->get_number());
        int64_t second_quotient  = 0;
        int64_t second_remainder = 0;
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        if (OB_FAIL(fetch_second_quotient(second_quotient, s))) {
          LOG_WARN("failed to fetch data from ObNumber obj sec", K(ret));
        } else if (OB_UNLIKELY(m < 0) ||
                   OB_UNLIKELY(m > 59) ||
                   OB_UNLIKELY(second_quotient < 0) ||
                   OB_UNLIKELY(second_quotient > 59) ||
                   OB_UNLIKELY(s.is_negative())) {
          // This part of null handling also refers to MySQL Item_func_maketime::get_time()
          results.datums_[i].set_null();
        } else if (OB_FAIL(fetch_second_remainder(second_quotient, second_remainder,
                                                  alloc_guard.get_allocator(), s))) {
          LOG_WARN("failed to fetch data from ObNumber obj sec", K(ret));
        } else {
          if (h < (-TIME_MAX_HOUR)) {
            h = -(TIME_MAX_HOUR + 1);
          }
          ob_time.parts_[DT_HOUR]  = h < 0 ? -h : h;
          ob_time.parts_[DT_MIN]   = m;
          ob_time.parts_[DT_SEC]   = (int32_t)second_quotient;  // value < 60, no impact
          ob_time.parts_[DT_USEC]  = (int32_t)second_remainder; // value < 999999, no impact
          if (OB_FAIL(ObTimeConverter::validate_time(ob_time))) {
            LOG_WARN("time value is invalid or out of range", K(ret));
          } else {
            int64_t value = ObTimeConverter::ob_time_to_time(ob_time);
            (void)ObTimeConverter::time_overflow_trunc(value);
            OX(results.datums_[i].set_time(h < 0 ? -(value): value));
          }
        }
      }
      eval_flags.set(i);
    }
  }
  LOG_DEBUG("eval_batch_maketime finished: batch mode", K(batch_size));
  return ret;
}

int ObExprMakeTime::eval_maketime(const ObExpr &expr, ObEvalCtx &ctx,
                                  ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObDatum * hour = NULL;
  ObDatum * minute = NULL;
  ObDatum * second = NULL;
  // Fetch input argument, hour, minute, second
  if (OB_FAIL(expr.eval_param_value(ctx, hour, minute, second))) {
    LOG_WARN("unexpected input value ", K(ret));
  } else if (OB_UNLIKELY(hour->is_null()) ||
             OB_UNLIKELY(minute->is_null()) ||
             OB_UNLIKELY(second->is_null())) {
    // Function maketime returns null when it matched above conditions.
    // Above logic refers to MySQL implemention Item_func_maketime::get_time
    // sql/item_timefunc.cc of version 5.6.
    result.set_null();
  } else {
    ObTime  ob_time(DT_TYPE_TIME);
    int32_t h = hour->get_int32();
    int32_t min = minute->get_int32();
    number::ObNumber sec(second->get_number());
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObArenaAllocator &alloc = alloc_guard.get_allocator();
    int64_t second_quotient = 0;
    int64_t second_remainder = 0;
    if (OB_FAIL(fetch_second_quotient(second_quotient, sec))) {
      LOG_WARN("failed to fetch data from ObNumber obj sec", K(ret));
    } else if (OB_UNLIKELY(min < 0) ||
        OB_UNLIKELY(min > 59) ||
        OB_UNLIKELY(second_quotient < 0) ||
        OB_UNLIKELY(second_quotient > 59) ||
        OB_UNLIKELY(sec.is_negative())) {
      result.set_null();
    } else if (OB_FAIL(fetch_second_remainder(second_quotient, second_remainder, alloc, sec))) {
      LOG_WARN("failed to fetch data from ObNumber obj sec", K(ret));
    } else {
      if (h < (-TIME_MAX_HOUR)) {
        h = -(TIME_MAX_HOUR + 1);
      }
      ob_time.parts_[DT_HOUR]  = h < 0 ? -h : h;
      ob_time.parts_[DT_MIN]   = min;
      ob_time.parts_[DT_SEC]   = (int32_t)second_quotient;  // value < 60, no impact
      ob_time.parts_[DT_USEC]  = (int32_t)second_remainder; // value < 999999, no impact
      if (OB_FAIL(ObTimeConverter::validate_time(ob_time))) {
        LOG_WARN("time value is invalid or out of range", K(ret));
      } else {
        int64_t value = ObTimeConverter::ob_time_to_time(ob_time);
        (void)ObTimeConverter::time_overflow_trunc(value);
        OX(result.set_time(h < 0 ? -(value): value));
      }
    }
  }

  return ret;
}

int ObExprMakeTime::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(3 == rt_expr.arg_cnt_);
  if(OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_maketime;
    rt_expr.eval_batch_func_ = eval_batch_maketime;
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase
