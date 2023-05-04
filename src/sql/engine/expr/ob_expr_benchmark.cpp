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

#include "sql/engine/expr/ob_expr_benchmark.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "common/ob_smart_call.h"
#include "sql/engine/expr/ob_expr_subquery_ref.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

ObExprBenchmark::ObExprBenchmark(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_BENCHMARK, N_BENCHMARK, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprBenchmark::calc_result_type2(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprResType &type2,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type2);
  UNUSED(type_ctx);
  type.set_int32();
  type1.set_calc_type(ObIntType);
  return ret;
}

int ObExprBenchmark::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  CK (2 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = ObExprBenchmark::eval_benchmark;
  if (!rt_expr.args_[0]->is_batch_result()) {
    rt_expr.eval_batch_func_ = eval_benchmark_batch;
  }
  return ret;
}

int ObExprBenchmark::eval_benchmark(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *loop_count;
  if (OB_FAIL(expr.args_[0]->eval(ctx, loop_count))) {
    LOG_WARN("failed to eval loop count", K(ret));
  } else if (loop_count->is_null() || loop_count->get_int() < 0) {
    expr_datum.set_null();
    if (!loop_count->is_null() && loop_count->get_int() < 0) {
      LOG_WARN("Incorrect count value for function benchmark", K(loop_count->get_int()));
    }
  } else {
    ObArray<ObExpr *> exprs_to_clear;
    int64_t loops = loop_count->get_int();
    ObDatum *tmp_datum = nullptr;
    if (OB_FAIL(collect_exprs(exprs_to_clear, expr, ctx))) {
      LOG_WARN("failed to collect expr", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < loops && OB_SUCC(THIS_WORKER.check_status()); ++i) {
        OZ (clear_all_flags(exprs_to_clear, ctx));
        OZ (expr.args_[1]->eval(ctx, tmp_datum));
      }
    }
    expr_datum.set_int32(0);
    exprs_to_clear.destroy();
  }
  return ret;
}

int ObExprBenchmark::eval_benchmark_batch(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObBitVector &skip,
                                          const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  ObDatum *loop_count;
  ObDatum *result = expr.locate_batch_datums(ctx);
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_FAIL(expr.args_[0]->eval(ctx, loop_count))) {
    LOG_WARN("failed to eval loop count", K(ret));
  } else if (loop_count->is_null() || loop_count->get_int() < 0) {
    for (int64_t j = 0; j < batch_size; ++j) {
      if (skip.at(j) || eval_flags.at(j)) {
        continue;
      }
      eval_flags.set(j);
      result[j].set_null();
    }
    if (!loop_count->is_null() && loop_count->get_int() < 0) {
      // compat with mysql, only throw a warning
      LOG_WARN("Incorrect count value for function benchmark", K(loop_count->get_int()));
    }
  } else {
    ObArray<ObExpr *> exprs_to_clear;
    int64_t loops = loop_count->get_int();
    ObDatum *tmp_datum = nullptr;
    if (OB_FAIL(collect_exprs(exprs_to_clear, expr, ctx))) {
      LOG_WARN("failed to collect expr", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (skip.at(j) || eval_flags.at(j)) {
          continue;
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < loops && OB_SUCC(THIS_WORKER.check_status()); ++i) {
          OZ (clear_all_flags(exprs_to_clear, ctx));
          OZ (expr.args_[1]->eval_batch(ctx, skip, batch_size));
        }
        OX (eval_flags.set(j));
        OX (result[j].set_int32(0));
      }
    }
  }
  return ret;
}

int ObExprBenchmark::collect_exprs(common::ObIArray<ObExpr *> &exprs,
                                   const ObExpr &root_expr,
                                   ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < root_expr.arg_cnt_; ++i) {
    ObEvalInfo &eval_flag = root_expr.args_[i]->get_eval_info(ctx);
    if (!eval_flag.evaluated_) {
      OZ (exprs.push_back(root_expr.args_[i]));
      OZ (SMART_CALL(collect_exprs(exprs, *root_expr.args_[i], ctx)));
    }
  }
  return ret;
}

int ObExprBenchmark::clear_all_flags(common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObEvalInfo &eval_flag = exprs.at(i)->get_eval_info(ctx);
    eval_flag.clear_evaluated_flag();
    if (T_REF_QUERY == exprs.at(i)->type_) {
      OZ (ObExprSubQueryRef::reset_onetime_expr(*exprs.at(i), ctx));
    }
  }
  return ret;
}

}//end namespace sql
}//end namespace oceanbase
