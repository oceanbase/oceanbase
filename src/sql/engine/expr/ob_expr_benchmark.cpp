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

#include "sql/engine/expr/ob_expr_benchmark.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "common/ob_smart_call.h"

namespace oceanbase {
using namespace common;

namespace sql {

ObExprBenchmark::ObExprBenchmark(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_BENCHMARK, N_BENCHMARK, 2, NOT_ROW_DIMENSION)
{}

int ObExprBenchmark::calc_result_type2(
    ObExprResType &type, ObExprResType &type1, ObExprResType &type2, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type2);
  UNUSED(type_ctx);
  type.set_int32();
  type1.set_calc_type(ObIntType);
  return ret;
}

int ObExprBenchmark::calc_result2(ObObj &res, const ObObj &obj1, const ObObj &obj2, ObExprCtx &expr_ctx) const
{
  UNUSED(res);
  UNUSED(obj1);
  UNUSED(obj2);
  UNUSED(expr_ctx);
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("expr benchmark is not implemented in old engine", K(ret));
  return ret;
}

int ObExprBenchmark::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  CK(2 == rt_expr.arg_cnt_);
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("expr benchmark is not implemented", K(ret));
  } else {
    rt_expr.eval_func_ = ObExprBenchmark::eval_benchmark;
  }
  return ret;
}

int ObExprBenchmark::eval_benchmark(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
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
        clear_all_flags(exprs_to_clear, ctx);
        OZ(expr.args_[1]->eval(ctx, tmp_datum));
      }
    }
    expr_datum.set_int32(0);
    exprs_to_clear.destroy();
  }
  return ret;
}

int ObExprBenchmark::collect_exprs(common::ObIArray<ObExpr *> &exprs, const ObExpr &root_expr, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < root_expr.arg_cnt_; ++i) {
    ObEvalInfo &eval_flag = root_expr.args_[i]->get_eval_info(ctx);
    if (!eval_flag.evaluated_) {
      OZ(exprs.push_back(root_expr.args_[i]));
      OZ(SMART_CALL(collect_exprs(exprs, *root_expr.args_[i], ctx)));
    }
  }
  return ret;
}

void ObExprBenchmark::clear_all_flags(common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx)
{
  for (int64_t i = 0; i < exprs.count(); ++i) {
    ObEvalInfo &eval_flag = exprs.at(i)->get_eval_info(ctx);
    eval_flag.clear_evaluated_flag();
  }
}

}  // end namespace sql
}  // end namespace oceanbase
