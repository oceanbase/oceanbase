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
#include "share/vector/ob_uniform_format.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObJoinVecSpec, ObOpSpec),
                    join_type_, other_join_conds_);

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

int ObJoinVecOp::calc_other_conds(bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  const ObIArray<ObExpr *> &conds = get_spec().other_join_conds_;
  ObDatum *cmp_res = NULL;
  ARRAY_FOREACH(conds, i) {
    if (OB_FAIL(conds.at(i)->eval(eval_ctx_, cmp_res))) {
      LOG_WARN("fail to calc other join condition", K(ret), K(*conds.at(i)));
    } else if (cmp_res->is_null() || 0 == cmp_res->get_int()) {
      is_match = false;
      break;
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
