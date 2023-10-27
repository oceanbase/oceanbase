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
#include "sql/engine/expr/ob_expr_format_bytes.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include <math.h>

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprFormatBytes::ObExprFormatBytes(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_FORMAT_BYTES, N_FORMAT_BYTES, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprFormatBytes::~ObExprFormatBytes()
{}

int ObExprFormatBytes::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret =  OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_varchar();
  type.set_length(LENGTH_FORMAT_BYTES);
  type.set_collation_level(common::CS_LEVEL_IMPLICIT);
  type.set_collation_type(common::CS_TYPE_UTF8MB4_GENERAL_CI);
  type1.set_calc_type(common::ObDoubleType);
  return ret;
}

int ObExprFormatBytes::eval_format_bytes(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("eval arg0 failed", K(ret));
  } else {
    if (OB_FAIL(eval_format_bytes_util(expr, res_datum, datum, ctx))){
      LOG_WARN("eval format_bytes unexpect error", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObExprFormatBytes::eval_format_bytes_batch(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const int64_t batch_size) {
  LOG_DEBUG("eval format bytes batch mode", K(batch_size));
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
      if (OB_FAIL(eval_format_bytes_util(expr, res_datum[i], datum, ctx, i))){
        LOG_WARN("eval format_bytes unexpect error", K(ret));
      } else {
        // do nothing
      }
      eval_flags.set(i);
    }
  }
  return ret;
}

int ObExprFormatBytes::eval_format_bytes_util(const ObExpr &expr, ObDatum &res_datum,
                                      ObDatum *param1, ObEvalCtx &ctx,  int64_t index)
{
  int ret = OB_SUCCESS;
  if (param1->is_null()) {
    res_datum.set_null();
  } else {
    double bytes = param1->get_double();
    double bytes_abs = std::abs(bytes);
    uint64_t divisor;
    int len;
    const char *unit;
    char *res_buf = NULL;
    if (OB_ISNULL(res_buf = expr.get_str_res_mem(ctx, VALUE_BUF_LEN, index))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      if (bytes_abs >= eib) {
        divisor = eib;
        unit = "EiB";
      } else if (bytes_abs >= pib) {
        divisor = pib;
        unit = "PiB";
      } else if (bytes_abs >= tib) {
        divisor = tib;
        unit = "TiB";
      } else if (bytes_abs >= gib) {
        divisor = gib;
        unit = "GiB";
      } else if (bytes_abs >= mib) {
        divisor = mib;
        unit = "MiB";
      } else if (bytes_abs >= kib) {
        divisor = kib;
        unit = "KiB";
      } else {
        divisor = 1;
        unit = "bytes";
      }
      if (divisor == 1) {
        len = sprintf(res_buf, "%4d %s", (int)bytes, unit);
      } else {
        double value = bytes / divisor;
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


int ObExprFormatBytes::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprFormatBytes::eval_format_bytes;
  rt_expr.eval_batch_func_ = ObExprFormatBytes::eval_format_bytes_batch;
  return ret;
}
}//namespace sql
}//namespace oceanbase
