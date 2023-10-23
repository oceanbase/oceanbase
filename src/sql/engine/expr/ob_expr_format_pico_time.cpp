/**
 * Copyright (c) 2023 OceanBase
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
#include "sql/engine/expr/ob_expr_format_pico_time.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include <math.h>

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprFormatPicoTime::ObExprFormatPicoTime(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_FORMAT_PICO_TIME, N_FORMAT_PICO_TIME, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprFormatPicoTime::~ObExprFormatPicoTime()
{}

int ObExprFormatPicoTime::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret =  OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_length(LENGTH_FORMAT_PICO_TIME);
  type.set_collation_level(common::CS_LEVEL_IMPLICIT);
  type.set_collation_type(common::CS_TYPE_UTF8MB4_GENERAL_CI);
  type1.set_calc_type(common::ObDoubleType);
  return ret;
}

int ObExprFormatPicoTime::eval_format_pico_time(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("eval arg0 failed", K(ret));
  } else {
    if (OB_FAIL(eval_format_pico_time_util(expr, res_datum, datum, ctx))){
      LOG_WARN("eval format_pico_time unexpect error", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObExprFormatPicoTime::eval_format_pico_time_batch(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const int64_t batch_size) {
  LOG_DEBUG("eval format pico time batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("eval arg0 failed", K(ret));
  } else {
    ObDatum *res_datum = expr.locate_batch_datums(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      ObDatum *datum= &expr.args_[0]->locate_expr_datum(ctx, i);
      if (OB_FAIL(eval_format_pico_time_util(expr, res_datum[i], datum, ctx, i))){
        LOG_WARN("eval format_pico_time unexpect error", K(ret));
      } else {
        // do nothing
      }
      eval_flags.set(i);
    }
  }
  return ret;
}

int ObExprFormatPicoTime::eval_format_pico_time_util(const ObExpr &expr, ObDatum &res_datum,
                                            ObDatum *param1, ObEvalCtx &ctx,  int64_t index)
{
  int ret = OB_SUCCESS;
  if (param1->is_null()) {
    res_datum.set_null();
  } else {
    double time_val = param1->get_double();
    double time_abs = std::abs(time_val);
    uint64_t divisor;
    int len;
    const char *unit;
    char *res_buf = NULL;
    if (OB_ISNULL(res_buf = expr.get_str_res_mem(ctx, VALUE_BUF_LEN, index))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      if (time_abs >= day) {
        divisor = day;
        unit = "d";
      } else if (time_abs >= hour) {
        divisor = hour;
        unit = "h";
      } else if (time_abs >= min) {
        divisor = min;
        unit = "min";
      } else if (time_abs >= sec) {
        divisor = sec;
        unit = "s";
      } else if (time_abs >= milli) {
        divisor = milli;
        unit = "ms";
      } else if (time_abs >= micro) {
        divisor = micro;
        unit = "us";
      } else if (time_abs >= nano) {
        divisor = nano;
        unit = "ns";
      } else {
        divisor = 1;
        unit = "ps";
      }
      if (divisor == 1) {
        len = sprintf(res_buf, "%3d %s", (int)time_val, unit);
      } else {
        double value = time_val / divisor;
        if (std::abs(value) >= 100000.0) {
          len = sprintf(res_buf, "%4.2e %s", value, unit);
        } else {
          len = sprintf(res_buf, "%4.2f %s", value, unit);
        }
      }
      res_datum.set_string(res_buf, len);
    }
  }
  return ret;
}


int ObExprFormatPicoTime::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprFormatPicoTime::eval_format_pico_time;
  rt_expr.eval_batch_func_ = ObExprFormatPicoTime::eval_format_pico_time_batch;
  return ret;
}
}//namespace sql
}//namespace oceanbase
