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

#include "sql/engine/join/ob_join_vec_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObJoinVecSpec, ObOpSpec),
                    join_type_, other_join_conds_);

#define VEC_FORMAT_SWITCH_CASE(VEC_FORMAT, vec_ptr, brs)          \
  case VEC_FORMAT: {                                              \
    for (int64_t i = 0; i < brs.size_; ++i) {                     \
        if (vec_ptr->is_null(i) || !vec_ptr->get_bool(i)) {       \
          brs.set_skip(i);                                        \
        }                                                         \
      }                                                           \
    break;                                                        \
  }

int ObJoinVecOp::inner_rescan()
{
  return ObOperator::inner_rescan();
}

int ObJoinVecOp::blank_row_batch(const ExprFixedArray &exprs, int64_t batch_size)
{
  int ret = OB_SUCCESS;
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < exprs.count(); col_idx++) {
    if (OB_FAIL(exprs.at(col_idx)->init_vector_default(eval_ctx_, batch_size))) {
      LOG_WARN("fail to init vector", K(ret));
    } else {
      ObIVector *vec = exprs.at(col_idx)->get_vector(eval_ctx_);
      if (OB_UNLIKELY(VEC_UNIFORM_CONST == exprs.at(col_idx)->get_format(eval_ctx_))) {
        reinterpret_cast<ObUniformFormat<true> *>(vec)->set_null(0);
      } else if (VEC_UNIFORM == exprs.at(col_idx)->get_format(eval_ctx_)) {
        reinterpret_cast<ObUniformFormat<false> *>(vec)->set_all_null(batch_size);
      } else {
        reinterpret_cast<ObBitmapNullVectorBase *>(vec)->get_nulls()->set_all(batch_size);
        reinterpret_cast<ObBitmapNullVectorBase *>(vec)->set_has_null();
      }
      exprs.at(col_idx)->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

//TODO shengle here need CONST_UNIFORM_FORMAT == !is_batch_result
// and exprs must not CONST_UNIFORM_FORMAT
void ObJoinVecOp::blank_row_batch_one(const ExprFixedArray &exprs)
{
  clear_datum_eval_flag();
  for (int64_t i = 0; i < exprs.count(); i++) {
    ObIVector *vec = exprs.at(i)->get_vector(eval_ctx_);
    VectorFormat format = exprs.at(i)->get_format(eval_ctx_);
    if (OB_UNLIKELY(is_uniform_format(format))) {
      reinterpret_cast<ObUniformBase *>(vec)->set_null(eval_ctx_.get_batch_idx());
    } else {
      reinterpret_cast<ObBitmapNullVectorBase *>(vec)->set_null(eval_ctx_.get_batch_idx());
    }
    exprs.at(i)->set_evaluated_flag(eval_ctx_);
  }
}

int ObJoinVecOp::calc_other_conds(const ObBitVector &skip, bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  const ObIArray<ObExpr *> &conds = get_spec().other_join_conds_;
  const int64_t batch_idx = eval_ctx_.get_batch_idx();
  EvalBound eval_bound(eval_ctx_.get_batch_size(), batch_idx, batch_idx + 1, false);
  ObIVector *res_vec = nullptr;
  ARRAY_FOREACH(conds, i) {
    if (OB_FAIL(conds.at(i)->eval_vector(eval_ctx_, skip, eval_bound))) {
      LOG_WARN("fail to calc other join condition", K(ret), K(*conds.at(i)));
    } else if (OB_ISNULL(res_vec = conds.at(i)->get_vector(eval_ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get source vector", K(ret), K(res_vec));
    } else if (res_vec->is_null(batch_idx) || 0 == res_vec->get_int(batch_idx)) {
      is_match = false;
      break;
    }
  }
  return ret;
}

int ObJoinVecOp::batch_calc_other_conds(ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &conds = get_spec().other_join_conds_;
  ARRAY_FOREACH(conds, i) {
    if (OB_FAIL(conds.at(i)->eval_vector(eval_ctx_, brs))) {
      LOG_WARN("fail to calc other join condition", K(ret), K(*conds.at(i)));
    } else {
      VectorHeader &vec_header = conds.at(i)->get_vector_header(eval_ctx_);
      common::ObIVector *vec = conds.at(i)->get_vector(eval_ctx_);
      switch(vec_header.format_) {
        VEC_FORMAT_SWITCH_CASE(VEC_FIXED, static_cast<ObFixedLengthBase *>(vec), brs);
        VEC_FORMAT_SWITCH_CASE(VEC_UNIFORM, static_cast<ObUniformFormat<false> *>(vec), brs);
        VEC_FORMAT_SWITCH_CASE(VEC_UNIFORM_CONST, static_cast<ObUniformFormat<true> *>(vec), brs);
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", K(ret), K(vec_header.format_));
      }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
