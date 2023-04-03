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

} // end namespace sql
} // end namespace oceanbase
