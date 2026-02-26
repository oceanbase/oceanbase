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

#define USING_LOG_PREFIX SQL_EXE
#include "ob_expr_decode_trace_id.h"
namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprDecodeTraceId::ObExprDecodeTraceId(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_DECODE_TRACE_ID, N_DECODE_TRACE_ID, 1,
                         ObValidForGeneratedColFlag::VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprDecodeTraceId::~ObExprDecodeTraceId()
{
}
int ObExprDecodeTraceId::calc_result_type1(ObExprResType &type,
                                ObExprResType &trace_id,
                                ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (!trace_id.is_null() && !trace_id.is_string_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument type", K(ret));
  } else {
    trace_id.set_calc_type_default_varchar();
    trace_id.set_calc_collation_level(CS_LEVEL_SYSCONST);
    trace_id.set_calc_length(OB_MAX_DATABASE_NAME_LENGTH);
    type.set_varchar();
    type.set_default_collation_type();
    type.set_collation_level(CS_LEVEL_SYSCONST);
    type.set_length(MAX_DECODE_TRACE_ID_RES_LEN);
  }
  return ret;
}
int ObExprDecodeTraceId::calc_decode_trace_id_expr(const ObExpr &expr, ObEvalCtx &ctx,
                               ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *trace_id_datum = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, trace_id_datum))) {
    LOG_WARN("eval trace_id failed", K(ret));
  } else if (OB_ISNULL(trace_id_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param datum is null pointer", K(ret));
  } else if (trace_id_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(calc_one_row(expr, ctx, *trace_id_datum, res_datum))) {
    LOG_WARN("calc trace_id failed", K(ret));
  }
  return ret;
}
int ObExprDecodeTraceId::calc_decode_trace_id_expr_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                             const ObBitVector &skip,
                                             const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatum *results = nullptr;
  ObDatum *trace_id_datum_array = nullptr;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
    LOG_WARN("batch eval trace_id failed", K(ret));
  } else if (OB_ISNULL(results = expr.locate_batch_datums(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("results datum is null pointer", K(ret));
  } else if (OB_ISNULL(trace_id_datum_array = expr.args_[0]->locate_batch_datums(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param datum is null pointer", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard guard(ctx);
    guard.set_batch_size(batch_size);
    for (int64_t idx = 0; OB_SUCC(ret) && idx < batch_size; ++idx) {
      guard.set_batch_idx(idx);
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (trace_id_datum_array[idx].is_null()) {
        results[idx].set_null();
        eval_flags.set(idx);
      } else if (OB_FAIL(calc_one_row(expr, ctx, trace_id_datum_array[idx], results[idx]))) {
        LOG_WARN("calc trace_id failed", K(ret));
      } else {
        eval_flags.set(idx);
      }
    }
  }
  return ret;
}

int ObExprDecodeTraceId::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract expr should have 1 params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of extract expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprDecodeTraceId::calc_decode_trace_id_expr;
    rt_expr.eval_batch_func_ = ObExprDecodeTraceId::calc_decode_trace_id_expr_batch;
  }
  return ret;
}
}
} // namespace oceanbase