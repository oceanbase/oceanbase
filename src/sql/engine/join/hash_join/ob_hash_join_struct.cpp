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

#include "sql/engine/join/hash_join/ob_hash_join_struct.h"

namespace oceanbase
{
namespace sql
{
int ObHJStoredRow::convert_one_row_to_exprs(const ExprFixedArray &exprs,
                                            ObEvalCtx &eval_ctx,
                                            const RowMeta &row_meta,
                                            const ObHJStoredRow *row,
                                            const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObExpr *expr = exprs.at(i);
    if (OB_UNLIKELY(expr->is_const_expr())) {
      continue;
    } else {
      ObIVector *vec = expr->get_vector(eval_ctx);
      if (OB_FAIL(vec->from_row(row_meta, row, batch_idx, i))) {
        LOG_WARN("fail to set row to vector", K(ret), K(batch_idx), K(i), K(*expr));
      }
      exprs.at(i)->set_evaluated_projected(eval_ctx);
    }
  }
  return ret;
}

int ObHJStoredRow::attach_rows(const ObExprPtrIArray &exprs,
                               ObEvalCtx &ctx,
                               const RowMeta &row_meta,
                               const ObHJStoredRow **srows,
                               const uint16_t selector[],
                               const int64_t size) {
  int ret = OB_SUCCESS;
  if (size <= 0) {
    // do nothing
  } else {
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < exprs.count(); col_idx++) {
      ObExpr *expr = exprs.at(col_idx);
      if (OB_FAIL(expr->init_vector_default(ctx, selector[size - 1] + 1))) {
        LOG_WARN("fail to init vector", K(ret));
      } else {
        ObIVector *vec = expr->get_vector(ctx);
        if (VEC_UNIFORM_CONST != vec->get_format()) {
          ret = vec->from_rows(row_meta,
                               reinterpret_cast<const ObCompactRow **>(srows),
                               selector, size, col_idx);
          expr->set_evaluated_projected(ctx);
        }
      }
    }
  }

  return ret;
}


} // end namespace sql
} // end namespace oceanbase
