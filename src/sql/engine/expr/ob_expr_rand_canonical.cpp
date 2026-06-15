/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rand_canonical.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
OB_SERIALIZE_MEMBER((ObExprRandCanonical, ObFuncExprOperator));

const uint64_t ObExprRandCanonical::ObExprRandCtx::max_value_ = 0x3FFFFFFFL;

ObExprRandCanonical::ObExprRandCtx::ObExprRandCtx() : seed1_(1), seed2_(1) {}

ObExprRandCanonical::ObExprRandCtx::~ObExprRandCtx() {}

void ObExprRandCanonical::ObExprRandCtx::set_seed(uint32_t seed) {
  seed1_ = static_cast<uint32_t>(seed * 0x10001L + 55555555L) % max_value_;
  seed2_ = static_cast<uint32_t>(seed * 0x10000001L) % max_value_;
}

void ObExprRandCanonical::ObExprRandCtx::get_next_random(double &res) {
  seed1_ = (seed1_ * 3 + seed2_) % max_value_;
  seed2_ = (seed1_ + seed2_ + 33) % max_value_;
  res = static_cast<double>(seed1_) / static_cast<double>(max_value_);
}

ObExprRandCanonical::ObExprRandCanonical(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_CK_RAND_CANONICAL, N_RAND_CANONICAL,
                         ZERO_OR_ONE, NOT_VALID_FOR_GENERATED_COL,
                         NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE,
                         INTERNAL_IN_ORACLE_MODE) {}

ObExprRandCanonical::~ObExprRandCanonical() {}

int ObExprRandCanonical::calc_result_typeN(
    ObExprResType &type, ObExprResType *types, int64_t param_num,
    common::ObExprTypeCtx &type_ctx) const {
  UNUSED(types);
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (param_num > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid number of arguments", K(param_num));
  } else {
    type.set_double();
  }
  return ret;
}

int ObExprRandCanonical::calc_random_expr_canonical(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    ObDatum &res_datum) {
  int ret = OB_SUCCESS;
  ObDatum *seed_datum = NULL;

  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObExprRandCtx *random_ctx = NULL;
  if (OB_ISNULL(random_ctx = static_cast<ObExprRandCtx *>(
                    exec_ctx.get_expr_op_ctx(op_id)))) {
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(op_id, random_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret), K(op_id));
    } else {
      uint32_t seed = 0;
      // use timestamp as the seed for rand expression
      seed = static_cast<uint32_t>(ObTimeUtility::current_time());
      random_ctx->set_seed(seed);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(random_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("random ctx is NULL", K(ret));
    } else {
      double rand_res = 0.0;
      random_ctx->get_next_random(rand_res);
      res_datum.set_double(rand_res);
    }
  }

  return ret;
}

template <typename ResVec>
int ObExprRandCanonical::vector_rand_canonical(VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  uint64_t op_id = expr.expr_ctx_id_;
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  ObExprRandCtx *random_ctx = NULL;

  if (OB_ISNULL(random_ctx = static_cast<ObExprRandCtx *>(
                    exec_ctx.get_expr_op_ctx(op_id)))) {
    if (OB_FAIL(exec_ctx.create_expr_op_ctx(op_id, random_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret), K(op_id));
    } else {
      uint32_t seed = 0;
      // use timestamp as the seed for rand expression
      seed = static_cast<uint32_t>(ObTimeUtility::current_time());
      random_ctx->set_seed(seed);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(random_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("random ctx is NULL", K(ret));
    } else {
      bool no_skip = bound.get_all_rows_active();
      if (no_skip) {
        // Fast path: no skip, no need to check eval_flags
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end();
             ++idx) {
          double rand_res = 0.0;
          random_ctx->get_next_random(rand_res);
          res_vec->set_double(idx, rand_res);
        }
      } else {
        // Slow path: need to check skip and eval_flags
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end();
             ++idx) {
          if (skip.at(idx) || eval_flags.at(idx)) {
            continue;
          } else {
            double rand_res = 0.0;
            random_ctx->get_next_random(rand_res);
            res_vec->set_double(idx, rand_res);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprRandCanonical::calc_random_expr_canonical_vector(
    VECTOR_EVAL_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  VectorFormat res_format = expr.get_format(ctx);

  if (VEC_FIXED == res_format) {
    ret = vector_rand_canonical<DoubleFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
  } else if (VEC_UNIFORM == res_format) {
    ret = vector_rand_canonical<DoubleUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
  } else {
    ret = vector_rand_canonical<ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
  }

  return ret;
}

int ObExprRandCanonical::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                 const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const {
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRandCanonical::calc_random_expr_canonical;
  rt_expr.eval_vector_func_ =
      ObExprRandCanonical::calc_random_expr_canonical_vector;
  return ret;
}
} /* namespace sql */
} /* namespace oceanbase */
