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
#include "sql/engine/expr/ob_expr_generator_func.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprGeneratorFunc::ObExprGeneratorFunc(common::ObIAllocator &alloc)
	: ObFuncExprOperator(alloc, T_FUN_SYS_GENERATOR, "generator", 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprGeneratorFunc::~ObExprGeneratorFunc()
{
}

int ObExprGeneratorFunc::calc_result_type1(ObExprResType &type,
                                           ObExprResType &limit,
                                           common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  limit.set_calc_type(ObIntType);
  type.set_int();
  return ret;
}

int ObExprGeneratorFunc::eval_next_value(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObExprGeneratorFuncCtx *generator_ctx = NULL;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  if (OB_ISNULL(generator_ctx = static_cast<ObExprGeneratorFuncCtx *>(
              exec_ctx.get_expr_op_ctx(op_id)))) {
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(op_id, generator_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret), K(op_id));
    } else if (OB_ISNULL(generator_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("generator ctx is NULL", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObDatum *limit_datum = NULL;
    if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected arg_cnt", K(ret), K(expr.arg_cnt_));
    } else if (OB_FAIL(expr.eval_param_value(ctx, limit_datum))) {
      LOG_WARN("expr.eval_param_value failed", K(ret));
    } else if (OB_UNLIKELY(limit_datum->is_null())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "generator function. The argument should be an integer.");
    } else {
      int64_t	limit = limit_datum->get_int();
      int64_t next_value_res = (++generator_ctx->curr_value_);
      if (OB_UNLIKELY(next_value_res > limit)) {
        ret = OB_ITER_END;
      } else {
        res_datum.set_int(next_value_res);
      }
    }
  }
  return ret;
}

int ObExprGeneratorFunc::cg_expr(ObExprCGCtx &expr_cg_ctx,
                             const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  if (OB_UNLIKELY(1 != raw_expr.get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count for in expr", K(ret));
  } else if (OB_ISNULL(raw_expr.get_param_expr(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null param expr", K(ret));
  } else if (!raw_expr.get_param_expr(0)->is_const_expr()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "generator function. The argument should be a constant integer");
  }
  rt_expr.eval_func_ = ObExprGeneratorFunc::eval_next_value;
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
