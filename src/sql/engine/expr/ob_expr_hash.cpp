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

#include "ob_expr_hash.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprHash::ObExprHash(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_FUN_SYS_HASH, N_HASH, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                   INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprHash::~ObExprHash()
{
}

int ObExprHash::calc_result_typeN(ObExprResType &type,
                                  ObExprResType *types,
                                  int64_t param_num,
                                  ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_uint64();
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
  return ret;
}

int ObExprHash::calc_hash_value_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = HASH_SEED;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ObDatum *datum = NULL;
    if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
      LOG_WARN("failed to eval datum", K(ret));
    } else {
      ObExprHashFuncType hash_func = expr.args_[i]->basic_funcs_->murmur_hash_v2_;
      if (OB_FAIL(hash_func(*datum, hash_value, hash_value))) {
        LOG_WARN("failed to do hash", K(ret));
      }
    }
  }
  res_datum.set_uint(hash_value);
  return ret;
}

int ObExprHash::calc_hash_value_expr_batch(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  uint64_t hash_seed = HASH_SEED;
  uint64_t *batch_hash_vals = reinterpret_cast<uint64_t *>(
                              ctx.frames_[expr.frame_idx_] + expr.res_buf_off_);
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    if (OB_FAIL(expr.args_[i]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("failed to eval batch datum", K(ret));
    } else {
      ObDatum *datums = expr.args_[i]->locate_batch_datums(ctx);
      if (OB_ISNULL(datums)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to locate batch datums", K(ret));
      } else {
        ObBatchDatumHashFunc hash_func = expr.args_[i]->basic_funcs_->murmur_hash_v2_batch_;
        hash_func(batch_hash_vals, datums, expr.args_[i]->is_batch_result(), skip, batch_size,
                  i > 0 ? batch_hash_vals: &hash_seed, i > 0);
      }
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      } else {
        expr.locate_expr_datum(ctx, i).set_uint(batch_hash_vals[i]);
        eval_flags.set(i);
      }
    }
  }
  return ret;
}

template <typename ResVec>
static int set_hash_value_vector(VECTOR_EVAL_FUNC_ARG_DECL, uint64_t *hash_vals)
{
  int ret = OB_SUCCESS;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObBitVector &my_skip = expr.get_pvt_skip(ctx);
  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (my_skip.at(idx)) {
      continue;
    }
    res_vec->set_uint(idx, hash_vals[idx]);
    eval_flags.set(idx);
  }
  return ret;
}

int ObExprHash::calc_hash_value_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  uint64_t hash_seed = HASH_SEED;
  uint64_t *hash_vals = reinterpret_cast<uint64_t *>(expr.get_res_buf(ctx));
  ObBitVector &my_skip = expr.get_pvt_skip(ctx);
  my_skip.deep_copy(skip, bound.start(), bound.end());
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  my_skip.bit_or(eval_flags, bound.start(), bound.end());
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    if (OB_FAIL(expr.args_[i]->eval_vector(ctx, my_skip, bound))) {
      LOG_WARN("failed to eval prarmemters", K(i), K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
      ObIVector *vec = expr.args_[i]->get_vector(ctx);
      ret = vec->murmur_hash_v3(*expr.args_[i], hash_vals, my_skip, bound, i > 0 ? hash_vals : &hash_seed, i > 0);
    }
    if (OB_SUCC(ret)) {
      VectorFormat res_format = expr.get_format(ctx);
      switch (res_format) {
        case VEC_FIXED:
          ret = set_hash_value_vector<ObFixedLengthFormat<uint64_t>>(VECTOR_EVAL_FUNC_ARG_LIST, hash_vals);
          break;
        case VEC_CONTINUOUS:
          ret = set_hash_value_vector<ObContinuousFormat>(VECTOR_EVAL_FUNC_ARG_LIST, hash_vals);
          break;
        case VEC_DISCRETE:
          ret = set_hash_value_vector<ObDiscreteFormat>(VECTOR_EVAL_FUNC_ARG_LIST, hash_vals);
          break;
        case VEC_UNIFORM:
          ret = set_hash_value_vector<ObUniformBase>(VECTOR_EVAL_FUNC_ARG_LIST, hash_vals);
          break;
        default:
          ret = set_hash_value_vector<ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST, hash_vals);
      }
    }
  }
  return ret;
}

int ObExprHash::cg_expr(ObExprCGCtx &expr_cg_ctx,
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(expr_cg_ctx);
  if (0 == rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument number, param should be more than one", K(ret));
  } else {
    rt_expr.eval_func_ = &calc_hash_value_expr;
    rt_expr.eval_batch_func_ = &calc_hash_value_expr_batch;
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_1_0) {
      rt_expr.eval_vector_func_ = &calc_hash_value_expr_vector;
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
