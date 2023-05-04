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
#include "lib/time/ob_time_utility.h" /* time */
#include "sql/engine/expr/ob_expr_random.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER((ObExprRandom, ObFuncExprOperator), is_seed_const_);

ObExprRandom::ObExprRandomCtx::ObExprRandomCtx()
	: gen_()
{
}

ObExprRandom::ObExprRandomCtx::~ObExprRandomCtx()
{
}

void ObExprRandom::ObExprRandomCtx::set_seed(uint64_t seed)
{
  gen_.seed(seed);
}

void ObExprRandom::ObExprRandomCtx::get_next_random(int64_t &res)
{
  uint64_t rd = gen_();
  res = rd + INT64_MIN;
}

ObExprRandom::ObExprRandom(common::ObIAllocator &alloc)
	: ObFuncExprOperator(alloc, T_FUN_SYS_RANDOM, "random", ZERO_OR_ONE, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
		is_seed_const_(true)
{
}

ObExprRandom::~ObExprRandom()
{
}

int ObExprRandom::calc_result_typeN(ObExprResType &type,
		                                ObExprResType *types,
		                                int64_t param_num,
																		common::ObExprTypeCtx &type_ctx) const
{
	UNUSED(types);
	UNUSED(type_ctx);
	int ret = OB_SUCCESS;
	if (param_num > 1) {
		ret = OB_INVALID_ARGUMENT;
		LOG_WARN("invalid number of arguments", K(param_num));
	} else {
		if(param_num == 1) {
			types[0].set_calc_type(ObIntType);
		}
		type.set_int();
	}
	return ret;
}

int ObExprRandom::assign(const ObExprOperator &other)
{
  int ret = OB_SUCCESS;
  const ObExprRandom *tmp_other = dynamic_cast<const ObExprRandom*>(&other);
  if (OB_UNLIKELY(NULL == tmp_other)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument. wrong type for other", K(ret), K(other));
  } else if (OB_LIKELY(this != tmp_other)) {
    if (OB_FAIL(ObExprOperator::assign(other))) {
      LOG_WARN("copy in Base class ObExprOperator failed", K(ret));
    } else {
      this->is_seed_const_ = tmp_other->is_seed_const_;
    }
  }
  return ret;
}

int ObExprRandom::calc_random_expr_const_seed(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *seed_datum = NULL;
  if (OB_UNLIKELY(0 != expr.arg_cnt_ && 1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg_cnt", K(ret), K(expr.arg_cnt_));
  } else if (1 == expr.arg_cnt_ && OB_FAIL(expr.eval_param_value(ctx, seed_datum))) {
    LOG_WARN("expr.eval_param_value failed", K(ret));
  } else {
    uint64_t op_id = expr.expr_ctx_id_;
    ObExecContext &exec_ctx = ctx.exec_ctx_;
    ObExprRandomCtx *random_ctx = NULL;
    if (OB_ISNULL(random_ctx = static_cast<ObExprRandomCtx *>(
                exec_ctx.get_expr_op_ctx(op_id)))) {
      if (OB_FAIL(exec_ctx.create_expr_op_ctx(op_id, random_ctx))) {
        LOG_WARN("failed to create operator ctx", K(ret), K(op_id));
      } else {
        uint64_t seed = 0;
        if (expr.arg_cnt_ == 1) {
          if(!seed_datum->is_null()) {
            seed = static_cast<uint64_t> (seed_datum->get_int());
          }
        } else {
          // use timestamp as the seed for rand expression
          seed = static_cast<uint64_t> (ObTimeUtility::current_time());
        }
        random_ctx->set_seed(seed);
      }
    }

    if (OB_SUCC(ret)) {
      int64_t rand_res = 0;
      random_ctx->get_next_random(rand_res);
      res_datum.set_int(rand_res);
    }
  }
  return ret;
}

int ObExprRandom::calc_random_expr_nonconst_seed(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *seed_datum = NULL;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg_cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, seed_datum))) {
    LOG_WARN("expr.eval_param_value failed", K(ret));
  } else {
    uint64_t op_id = expr.expr_ctx_id_;
    ObExecContext &exec_ctx = ctx.exec_ctx_;
    ObExprRandomCtx *random_ctx = NULL;
		if (OB_ISNULL(random_ctx = static_cast<ObExprRandomCtx *>(
            exec_ctx.get_expr_op_ctx(op_id)))) {
			if (OB_FAIL(exec_ctx.create_expr_op_ctx(op_id, random_ctx))) {
				LOG_WARN("failed to create operator ctx", K(ret), K(op_id));
			}
		}
		if (OB_SUCC(ret)) {
      if (OB_ISNULL(random_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("random ctx is NULL", K(ret));
      } else {
				uint64_t seed = 0;
				if(!seed_datum->is_null()) {
					seed = static_cast<uint64_t>(seed_datum->get_int());
				}
        int64_t rand_res = 0;
				random_ctx->set_seed(seed);
        random_ctx->get_next_random(rand_res);
        res_datum.set_int(rand_res);
      }
		}
  }
  return ret;
}

int ObExprRandom::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (is_seed_const_) {
    rt_expr.eval_func_ = ObExprRandom::calc_random_expr_const_seed;
  } else {
    rt_expr.eval_func_ = ObExprRandom::calc_random_expr_nonconst_seed;
  }
  return ret;
}
} /* namespace sql */
} /* namespace oceanbase */

