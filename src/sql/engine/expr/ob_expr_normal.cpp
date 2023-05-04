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
#include "sql/engine/expr/ob_expr_normal.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{


int ObExprNormal::ObExprNormalCtx::initialize(ObEvalCtx &ctx, const ObExpr &expr)
{
  int ret = OB_SUCCESS;
  ObDatum &p1 = expr.locate_param_datum(ctx, 0);
  ObDatum &p2 = expr.locate_param_datum(ctx, 1);
  if (p1.is_null() || p2.is_null()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "normal function. first and second argument must be constant expression.");
  } else {
    double mean = p1.get_double();
    double stddev = p2.get_double();
    std::normal_distribution<double>::param_type param(mean, stddev);
    normal_dist_.param(param);
  }
  return ret;
}


int ObExprNormal::ObExprNormalCtx::generate_next_value(int64_t seed, double &result)
{
  normal_dist_.reset();
  gen_.seed(static_cast<uint64_t>(seed));
  result = normal_dist_(gen_);
  return OB_SUCCESS;
}

ObExprNormal::ObExprNormal(common::ObIAllocator &alloc)
	: ObFuncExprOperator(alloc, T_FUN_SYS_NORMAL, "normal", 3, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprNormal::~ObExprNormal()
{
}

int ObExprNormal::calc_result_type3(ObExprResType &result_type,
                                  ObExprResType &mean,
                                  ObExprResType &stddev,
                                  ObExprResType &rand_expr,
                                  common::ObExprTypeCtx &type_ctx) const
{
	UNUSED(type_ctx);
	int ret = OB_SUCCESS;
  mean.set_calc_type(ObDoubleType);
  stddev.set_calc_type(ObDoubleType);
  rand_expr.set_calc_type(ObIntType);
  result_type.set_double();
	return ret;
}

int ObExprNormal::eval_next_value(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObExprNormalCtx *normal_ctx = NULL;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;

  if (OB_FAIL(expr.eval_param_value(ctx))) {
      LOG_WARN("expr.eval_param_value failed", K(ret));
  } else if (OB_ISNULL(normal_ctx = static_cast<ObExprNormalCtx *>(
              exec_ctx.get_expr_op_ctx(op_id)))) {
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(op_id, normal_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret), K(op_id));
    } else if (OB_ISNULL(normal_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("normal ctx is NULL", K(ret));
    } else if (OB_FAIL(normal_ctx->initialize(ctx, expr))) {
      LOG_WARN("fail init normal context", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObDatum &rand_val = expr.locate_param_datum(ctx, 2);
    if (OB_UNLIKELY(rand_val.is_null())) {
        res_datum.set_null();
    } else {
      int64_t	seed = rand_val.get_int();
      double next_value_res = 0.0;
      if (OB_FAIL(normal_ctx->generate_next_value(seed, next_value_res))) {
        LOG_WARN("fail generate next normal value", K(ret), K(seed));
      } else {
        res_datum.set_double(next_value_res);
      }
    }
  }
  return ret;
}

int ObExprNormal::cg_expr(ObExprCGCtx &expr_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  if (OB_UNLIKELY(3 != raw_expr.get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param count for in expr", K(ret));
  } else if (OB_ISNULL(raw_expr.get_param_expr(0)) ||
             OB_ISNULL(raw_expr.get_param_expr(1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null param expr", K(ret));
  } else if (!raw_expr.get_param_expr(0)->is_const_expr()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "normal function's first argument. must be a constant expression.");
  } else if (!raw_expr.get_param_expr(1)->is_const_expr()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "normal function's second argument. must be a constant expression.");
  }

  rt_expr.eval_func_ = ObExprNormal::eval_next_value;
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
