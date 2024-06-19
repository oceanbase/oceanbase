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

#define USING_LOG_PREFIX SQL

#include "ob_batch_eval_util.h"

namespace oceanbase
{
namespace sql
{

int binary_operand_batch_eval(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              const ObBitVector &skip,
                              const int64_t size,
                              const bool null_short_circuit)
{
  int ret = 0;
  const ObExpr &left = *expr.args_[0];
  const ObExpr &right = *expr.args_[1];
  if (null_short_circuit) {
    if (OB_FAIL(left.eval_batch(ctx, skip, size))) {
      LOG_WARN("batch evaluate failed", K(ret), K(expr));
    } else if (left.is_batch_result()) {
      const ObBitVector *rskip = &skip;
      if (!left.get_eval_info(ctx).notnull_) {
        ObBitVector &my_skip = expr.get_pvt_skip(ctx);
        rskip = &my_skip;
        my_skip.deep_copy(skip, size);
        const ObDatum *ldatums = left.locate_batch_datums(ctx);
        for (int64_t i = 0; i < size; i++) {
          if (!my_skip.at(i) && ldatums[i].is_null()) {
            my_skip.set(i);
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (rskip->is_all_true(size)) {
        // If rskip is all true, the right expr does not need to be evaluated.
      } else if (OB_FAIL(right.eval_batch(ctx, *rskip, size))) {
        LOG_WARN("batch evaluated failed", K(ret), K(right));
      }
    } else {
      if (!left.locate_expr_datum(ctx).is_null()) {
        if (OB_FAIL(right.eval_batch(ctx, skip, size))) {
          LOG_WARN("batch evaluate failed", K(ret), K(right));
        }
      }
    }
  } else {
    if (OB_FAIL(left.eval_batch(ctx, skip, size))
        || OB_FAIL(right.eval_batch(ctx, skip, size))) {
      LOG_WARN("batch evaluate failed", K(ret), K(expr));
    }
  }
  return ret;
}

int binary_operand_vector_eval(const ObExpr &expr,
                              ObEvalCtx &ctx,
                              const ObBitVector &skip,
                              EvalBound &bound,
                              const bool null_short_circuit,
                              bool &right_evaluated)
{
  int ret = 0;
  right_evaluated = true;
  const ObExpr &left = *expr.args_[0];
  const ObExpr &right = *expr.args_[1];
  const ObBitVector *rskip = &skip;
  if (null_short_circuit) {
    if (OB_FAIL(left.eval_vector(ctx, skip, bound))) {
      LOG_WARN("batch evaluate failed", K(ret), K(expr));
    } else if (left.get_format(ctx) != VEC_UNIFORM_CONST) {
      if (OB_LIKELY(!left.get_vector(ctx)->has_null())) {
        if (OB_FAIL(right.eval_vector(ctx, *rskip, bound))) {
          LOG_WARN("batch evaluated failed", K(ret), K(right));
        }
      } else {
        ObBitVector &my_skip = expr.get_pvt_skip(ctx);
        rskip = &my_skip;
        if (OB_LIKELY(bound.get_all_rows_active())) {
          if (OB_LIKELY(left.get_format(ctx) != VEC_UNIFORM)) {
            my_skip.deep_copy(
              *static_cast<ObBitmapNullVectorBase *>(left.get_vector(ctx))->get_nulls(),
              bound.start(), bound.end());
          } else {
            my_skip.reset(bound.batch_size());
            ObUniformBase *left_vec = static_cast<ObUniformBase *>(left.get_vector(ctx));
            for (int i = bound.start(); i < bound.end(); i++) {
              if (left_vec->is_null(i)) { my_skip.set(i); }
            }
          }
        } else if (OB_LIKELY(left.get_format(ctx) != VEC_UNIFORM)) {
          const ObBitVector *nulls_map = static_cast<ObBitmapNullVectorBase *>(left.get_vector(ctx))->get_nulls();
          my_skip.deep_copy(skip, bound.start(), bound.end());
          my_skip.bit_or(*nulls_map, bound.start(), bound.end());
        } else {
          my_skip.deep_copy(skip, bound.start(), bound.end());
          ObUniformBase *left_vec = static_cast<ObUniformBase *>(left.get_vector(ctx));
          for (int i = bound.start(); i < bound.end(); i++) {
            if (left_vec->is_null(i)) { my_skip.set(i);}
          }
        }
        bound.set_all_row_active(false);
      }
      if (OB_FAIL(ret)) {
      } else if (rskip->is_all_true(bound.start(), bound.end())) {
        right_evaluated = false;
        // If rskip is all true, the right expr does not need to be evaluated.
      } else if (OB_FAIL(right.eval_vector(ctx, *rskip, bound))) {
        LOG_WARN("batch evaluated failed", K(ret), K(right));
      }
    } else {
      if (!static_cast<ObVectorBase *>(left.get_vector(ctx))->is_null(0)) {
        if (OB_FAIL(right.eval_vector(ctx, skip, bound))) {
          LOG_WARN("batch evaluate failed", K(ret), K(right));
        }
      } else {
        right_evaluated = false;
        bound.set_all_row_active(false);
      }
    }
  } else {
    if (OB_FAIL(left.eval_vector(ctx, skip, bound))
        || OB_FAIL(right.eval_vector(ctx, skip, bound))) {
      LOG_WARN("batch evaluate failed", K(ret), K(expr));
    }
  }
  if (OB_SUCC(ret)) {
    SQL_LOG(DEBUG, "expr.args_[0]", K(ToStrVectorHeader(*expr.args_[0], ctx, &skip, bound)));
    SQL_LOG(DEBUG, "expr.args_[1]", K(ToStrVectorHeader(*expr.args_[1], ctx, rskip, bound)));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
