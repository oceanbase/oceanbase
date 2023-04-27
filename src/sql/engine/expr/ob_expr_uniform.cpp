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

#include "sql/engine/expr/ob_expr_uniform.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/json_type/ob_json_base.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObExprUniform::ObExprUniformIntCtx::initialize(ObEvalCtx &ctx, const ObExpr &expr)
{
  int ret = OB_SUCCESS;
  ObDatum &p1 = expr.locate_param_datum(ctx, 0);
  ObDatum &p2 = expr.locate_param_datum(ctx, 1);
  if (p1.is_null() || p2.is_null()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "uniform min/max value. must be a constant expression.");
  } else {
    int64_t a = p1.get_int();
    int64_t b = p2.get_int();
    std::uniform_int_distribution<int64_t>::param_type param(a, b);
    int_dist_.param(param);
    if (a > b) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "uniform min/max value. min value must be smaller than or equal to max value.");
    }
  }
  return ret;
}

int ObExprUniform::ObExprUniformIntCtx::generate_next_value(int64_t seed, int64_t &res)
{
  gen_.seed(seed);
  res = int_dist_(gen_);
  return OB_SUCCESS;
}

int ObExprUniform::ObExprUniformRealCtx::initialize(ObEvalCtx &ctx, const ObExpr &expr)
{
  int ret = OB_SUCCESS;
  ObDatum &p1 = expr.locate_param_datum(ctx, 0);
  ObDatum &p2 = expr.locate_param_datum(ctx, 1);
  if (p1.is_null() || p2.is_null()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "uniform min/max value. must be a constant number.");
  } else {
    double a = p1.get_double();
    double b = p2.get_double();
    std::uniform_real_distribution<double>::param_type param(a, b);
    real_dist_.param(param);
    if (a > b) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "uniform min/max value. min value must be smaller than or equal to max value.");
    }
  }
  return ret;
}

int ObExprUniform::ObExprUniformRealCtx::generate_next_value(int64_t seed, double &res)
{
  gen_.seed(seed);
  res = real_dist_(gen_);
  return OB_SUCCESS;
}

ObExprUniform::ObExprUniform(common::ObIAllocator &alloc)
	: ObFuncExprOperator(alloc, T_FUN_SYS_UNIFORM, "uniform", 3, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprUniform::~ObExprUniform()
{
}

int ObExprUniform::calc_result_type3(ObExprResType &result_type,
                                     ObExprResType &min_type,
                                     ObExprResType &max_type,
                                     ObExprResType &rand_expr,
                                     common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    min_type.set_calc_type(ObDoubleType);
    max_type.set_calc_type(ObDoubleType);
    result_type.set_number();
    result_type.set_precision(PRECISION_UNKNOWN_YET);
    result_type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
  } else if (ob_is_integer_type(min_type.get_type()) && ob_is_integer_type(max_type.get_type())) {
    min_type.set_calc_type(ObIntType);
    max_type.set_calc_type(ObIntType);
    result_type.set_int();
  } else {
    min_type.set_calc_type(ObDoubleType);
    max_type.set_calc_type(ObDoubleType);
    result_type.set_double();
  }
  rand_expr.set_calc_type(ObIntType);
  return ret;
}

int ObExprUniform::eval_next_int_value(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObExprUniformIntCtx *uniform_ctx = NULL;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;

  if (OB_FAIL(expr.eval_param_value(ctx))) {
      LOG_WARN("expr.eval_param_value failed", K(ret));
  } else if (OB_ISNULL(uniform_ctx = static_cast<ObExprUniformIntCtx *>(
              exec_ctx.get_expr_op_ctx(op_id)))) {
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(op_id, uniform_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret), K(op_id));
    } else if (OB_ISNULL(uniform_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("uniform ctx is NULL", K(ret));
    } else if (OB_FAIL(uniform_ctx->initialize(ctx, expr))) {
      LOG_WARN("fail init uniform context", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObDatum &rand_val = expr.locate_param_datum(ctx, 2);
    if (OB_UNLIKELY(rand_val.is_null())) {
        res_datum.set_null();
    } else {
      int64_t	seed = rand_val.get_int();
      int64_t res = 0;
      if (OB_FAIL(uniform_ctx->generate_next_value(seed, res))) {
        LOG_WARN("fail generate next uniform value", K(ret), K(seed));
      } else {
        res_datum.set_int(res);
      }
    }
  }
  return ret;
}

int ObExprUniform::eval_next_real_value(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObExprUniformRealCtx *uniform_ctx = NULL;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;

  if (OB_FAIL(expr.eval_param_value(ctx))) {
      LOG_WARN("expr.eval_param_value failed", K(ret));
  } else if (OB_ISNULL(uniform_ctx = static_cast<ObExprUniformRealCtx *>(
              exec_ctx.get_expr_op_ctx(op_id)))) {
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(op_id, uniform_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret), K(op_id));
    } else if (OB_ISNULL(uniform_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("uniform ctx is NULL", K(ret));
    } else if (OB_FAIL(uniform_ctx->initialize(ctx, expr))) {
      LOG_WARN("fail init uniform context", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObDatum &rand_val = expr.locate_param_datum(ctx, 2);
    if (OB_UNLIKELY(rand_val.is_null())) {
        res_datum.set_null();
    } else {
      int64_t	seed = rand_val.get_int();
      double res = 0.0;
      if (OB_FAIL(uniform_ctx->generate_next_value(seed, res))) {
        LOG_WARN("fail generate next uniform value", K(ret), K(seed));
      } else {
        res_datum.set_double(res);
      }
    }
  }
  return ret;
}

// No need to introduce a native number solution. just reuse double solution,
// and convert the double result into number format
int ObExprUniform::eval_next_number_value(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObExprUniformRealCtx *uniform_ctx = NULL;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;

  if (OB_FAIL(expr.eval_param_value(ctx))) {
      LOG_WARN("expr.eval_param_value failed", K(ret));
  } else if (OB_ISNULL(uniform_ctx = static_cast<ObExprUniformRealCtx *>(
              exec_ctx.get_expr_op_ctx(op_id)))) {
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(op_id, uniform_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret), K(op_id));
    } else if (OB_ISNULL(uniform_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("uniform ctx is NULL", K(ret));
    } else if (OB_FAIL(uniform_ctx->initialize(ctx, expr))) {
      LOG_WARN("fail init uniform context", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObDatum &rand_val = expr.locate_param_datum(ctx, 2);
    if (OB_UNLIKELY(rand_val.is_null())) {
        res_datum.set_null();
    } else {
      int64_t	seed = rand_val.get_int();
      double d = 0.0;
      number::ObNumber res;
      char local_buff[number::ObNumber::MAX_BYTE_LEN];
      ObDataBuffer local_alloc(local_buff, number::ObNumber::MAX_BYTE_LEN);
      if (OB_FAIL(uniform_ctx->generate_next_value(seed, d))) {
        LOG_WARN("fail generate next uniform value", K(ret), K(seed));
      } else if (OB_FAIL(ObJsonBaseUtil::double_to_number(d, local_alloc, res))) {
        LOG_WARN("fail convert double to number", K(seed), K(d));
      } else {
        res_datum.set_number(res);
      }
    }
  }
  return ret;
}


int ObExprUniform::cg_expr(ObExprCGCtx &expr_cg_ctx,
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
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "uniform function's first argument. must be a constant expression.");
  } else if (!raw_expr.get_param_expr(1)->is_const_expr()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "uniform function's second argument. must be a constant expression.");
  }

  if (lib::is_oracle_mode()) {
    rt_expr.eval_func_ = ObExprUniform::eval_next_number_value;
  } else if (ob_is_integer_type(raw_expr.get_param_expr(0)->get_data_type())
      && ob_is_integer_type(raw_expr.get_param_expr(1)->get_data_type())) {
    rt_expr.eval_func_ = ObExprUniform::eval_next_int_value;
  } else {
    rt_expr.eval_func_ = ObExprUniform::eval_next_real_value;
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
