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
#include "sql/engine/expr/ob_array_expr_utils.h"

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

int ObHJStoredRow::convert_rows_to_exprs(const ExprFixedArray &exprs, ObEvalCtx &eval_ctx,
    const RowMeta &row_meta, const ObHJStoredRow **rows, const uint16_t *sel,
    const uint16_t sel_cnt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObExpr *expr = exprs.at(i);
    if (OB_UNLIKELY(expr->is_const_expr())) {
      continue;
    } else {
      ObIVector *vec = expr->get_vector(eval_ctx);
      if (OB_FAIL(vec->from_rows(
              row_meta, reinterpret_cast<const ObCompactRow **>(rows), sel, sel_cnt, i))) {
        LOG_WARN("fail to set rows to vector", K(ret), K(i), K(*expr));
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

int ObHJStoredRow::attach_rows(const ObExprPtrIArray &exprs,
                               ObEvalCtx &ctx,
                               const RowMeta &row_meta,
                               const ObHJStoredRow **srows,
                               const int64_t size) {
  int ret = OB_SUCCESS;
  if (size <= 0) {
    // do nothing
  } else {
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < exprs.count(); col_idx++) {
      ObExpr *expr = exprs.at(col_idx);
      if (OB_FAIL(expr->init_vector_default(ctx, size))) {
        LOG_WARN("fail to init vector", K(ret));
      } else {
        ObIVector *vec = expr->get_vector(ctx);
        if (VEC_UNIFORM_CONST != vec->get_format()) {
          ret =
            vec->from_rows(row_meta, reinterpret_cast<const ObCompactRow **>(srows), size, col_idx);
          expr->set_evaluated_projected(ctx);
        }
      }
    }
  }

  return ret;
}

int JoinTableCtx::prepare_part_rows_array(uint64_t row_num, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (nullptr == left_part_rows_) {
    void *alloc_buf = allocator->alloc(sizeof(ModulePageAllocator));
    void *array_buf = allocator->alloc(sizeof(PartRowsArray));
    if (OB_ISNULL(alloc_buf) || OB_ISNULL(array_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (OB_NOT_NULL(alloc_buf)) {
        allocator->free(alloc_buf);
      }
      if (OB_NOT_NULL(array_buf)) {
        allocator->free(array_buf);
      }
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      page_alloc_ = new (alloc_buf) ModulePageAllocator(*allocator);
      page_alloc_->set_label("HtOpAlloc");
      left_part_rows_ = new (array_buf) PartRowsArray(*page_alloc_);
      if (OB_FAIL(left_part_rows_->reserve(row_num))) {
        LOG_WARN("failed to init left_part_rows", K(ret), K(row_num));
      }
    }
  } else {
    left_part_rows_->reuse();
    if (OB_FAIL(left_part_rows_->reserve(row_num))) {
      LOG_WARN("failed to init left_part_rows", K(ret), K(row_num));
    }
  }
  if (OB_SUCC(ret)) {
    read_row_idx_ = 0;
  }
  return ret;
}

int JoinTableCtx::insert_left_part_rows(uint64_t row_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(left_part_rows_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left part rows is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_num; i++) {
      left_part_rows_->push_back(stored_rows_[i]);
    }
  }
  return ret;
}

int JoinTableCtx::get_unmatched_rows(OutputInfo &output_info)
{
  int ret = OB_SUCCESS;
  int64_t batch_idx = 0;
  if (OB_ISNULL(left_part_rows_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left part rows is null", K(ret));
  }
  while (OB_SUCC(ret) && batch_idx < *max_output_cnt_) {
    if (read_row_idx_ >= left_part_rows_->count()) {
      ret = OB_ITER_END;
      break;
    }
    if (!left_part_rows_->at(read_row_idx_)->is_match(build_row_meta_)) {
      output_info.left_result_rows_[batch_idx++] = left_part_rows_->at(read_row_idx_);
    }
    read_row_idx_ += 1;
  }
  output_info.selector_cnt_ = batch_idx;
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
