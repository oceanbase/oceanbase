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
#include "sql/engine/expr/ob_expr_zipf.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{


int ObExprZipf::ObExprZipfCtx::initialize(ObEvalCtx &ctx, const ObExpr &expr)
{
  int ret = OB_SUCCESS;
  ObDatum &p1 = expr.locate_param_datum(ctx, 0);
  ObDatum &p2 = expr.locate_param_datum(ctx, 1);
  double alpha = 0.0;
  int64_t n = 0;
  probe_cp_.set_label("ZipfFunc");
  if (p1.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "zipf first argument. must be a constant expression no less than 1");
  } else if (p2.is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "zipf second argument. must be a constant expression between 1 and 16777215 inclusive");
  } else if (FALSE_IT(alpha = p1.get_double())) {
  } else if (FALSE_IT(n = p2.get_int())) {
  } else if (alpha < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "zipf first argument. must be a constant expression no less than 1");
  } else if (n < 1 || n > 16777215) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "zipf second argument. must be a constant expression between 1 and 16777215 inclusive");
  } else if (OB_FAIL(probe_cp_.reserve(n))) {
    LOG_WARN("fail allocate memory", K(ret), K(n));
  } else {
    double acc_sum = 0.0;
    for (int64_t i = 1; OB_SUCC(ret) && i <= n; ++i) {
      double f = 1.0 / pow(i, alpha);
      acc_sum += f;
      if (OB_FAIL(probe_cp_.push_back(acc_sum))) {
        LOG_WARN("fail push value", K(acc_sum), K(f), K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
      probe_cp_[i] = probe_cp_[i] / acc_sum;
    }
    // Make sure the last cumulative probability is one.
    probe_cp_[n -1] = 1;
  }
  return ret;
}

int ObExprZipf::ObExprZipfCtx::generate_next_value(int64_t seed, int64_t &result)
{
  gen_.seed(static_cast<uint64_t>(seed));
  double normalized_seed = static_cast<double>(gen_()) / UINT64_MAX;
  auto pos = std::lower_bound(probe_cp_.begin(), probe_cp_.end(), normalized_seed);
  result = pos - probe_cp_.begin();
  return OB_SUCCESS;
}

ObExprZipf::ObExprZipf(common::ObIAllocator &alloc)
	: ObFuncExprOperator(alloc, T_FUN_SYS_ZIPF, "zipf", 3, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprZipf::~ObExprZipf()
{
}

int ObExprZipf::calc_result_type3(ObExprResType &result_type,
                                  ObExprResType &exponent,
                                  ObExprResType &size,
                                  ObExprResType &rand_expr,
                                  common::ObExprTypeCtx &type_ctx) const
{
	UNUSED(type_ctx);
	int ret = OB_SUCCESS;
  exponent.set_calc_type(ObDoubleType);
  size.set_calc_type(ObIntType);
  rand_expr.set_calc_type(ObIntType);
  result_type.set_int();
  ObAccuracy default_acc = ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][ObIntType];
  result_type.set_accuracy(default_acc);
	return ret;
}

int ObExprZipf::eval_next_value(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObExprZipfCtx *zipf_ctx = NULL;
  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;

  if (OB_FAIL(expr.eval_param_value(ctx))) {
      LOG_WARN("expr.eval_param_value failed", K(ret));
  } else if (OB_ISNULL(zipf_ctx = static_cast<ObExprZipfCtx *>(
              exec_ctx.get_expr_op_ctx(op_id)))) {
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(op_id, zipf_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret), K(op_id));
    } else if (OB_ISNULL(zipf_ctx)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("zipf ctx is NULL", K(ret));
    } else if (OB_FAIL(zipf_ctx->initialize(ctx, expr))) {
      LOG_WARN("fail init zipf context", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObDatum &rand_val = expr.locate_param_datum(ctx, 2);
    if (OB_UNLIKELY(rand_val.is_null())) {
        res_datum.set_null();
    } else {
      int64_t	seed = rand_val.get_int();
      int64_t next_value_res = 0;
      if (OB_FAIL(zipf_ctx->generate_next_value(seed, next_value_res))) {
        LOG_WARN("fail generate next zipf value", K(ret), K(seed));
      } else {
        res_datum.set_int(next_value_res);
      }
    }
  }
  return ret;
}

int ObExprZipf::cg_expr(ObExprCGCtx &expr_cg_ctx,
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
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "zipf first argument. must be a constant expression no less than 1");
  } else if (!raw_expr.get_param_expr(1)->is_const_expr()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "zipf second argument. must be a constant expression between 1 and 16777215 inclusive");
  }
  rt_expr.eval_func_ = ObExprZipf::eval_next_value;
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
