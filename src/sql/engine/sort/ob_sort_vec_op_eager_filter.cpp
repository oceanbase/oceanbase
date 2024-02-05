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
#include "sql/engine/sort/ob_sort_vec_op_eager_filter.h"
namespace oceanbase {
namespace sql {
template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpEagerFilter<Compare, Store_Row, has_addon>::init_output_brs(
    const int64_t max_batch_size) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(output_brs_.skip_)) {
    void *buf = allocator_.alloc(ObBitVector::memory_size(max_batch_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      output_brs_.skip_ = to_bit_vector(buf);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "output_brs_ init twice", K(ret));
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpEagerFilter<Compare, Store_Row, has_addon>::filter(
    common::ObFixedArray<ObExpr *, common::ObIAllocator> &exprs,
    ObEvalCtx &eval_ctx, const int64_t start_pos,
    const ObBatchRows &input_brs) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSortVecOpEagerFilter is not initialized", K(ret));
  } else if (is_by_pass()) {
    // do nothing
  } else {
    output_brs_.copy(&input_brs);
    if (bucket_heap_->count() < bucket_num_) {
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
      batch_info_guard.set_batch_size(output_brs_.size_);
      for (int64_t i = start_pos; OB_SUCC(ret) && i < output_brs_.size_; ++i) {
        if (output_brs_.skip_->exist(i)) {
          continue;
        }
        batch_info_guard.set_batch_idx(i);
        const bool less = (*comp_)(bucket_heap_->top(), eval_ctx);
        if (!less) {
          output_brs_.set_skip(i);
        }
        ret = comp_->ret_;
      }
      if (ret != OB_SUCCESS) {
        LOG_WARN("failed to eager filter rows in topn operator", K(ret));
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpEagerFilter<Compare, Store_Row, has_addon>::update_filter(
    const Store_Row *bucket_head_row, bool &updated) {
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSortVecOpEagerFilter is not initialized", K(ret));
  } else if (is_by_pass()) {
    updated = false;
  } else {
    Store_Row *reuse_row = nullptr;
    if (bucket_heap_->count() < bucket_num_) {
      if (OB_FAIL(store_row_factory_.copy_to_row(bucket_head_row, reuse_row))) {
        LOG_WARN("failed to generate new row", K(ret));
      } else {
        if (OB_FAIL(bucket_heap_->push(reuse_row))) {
          ret = OB_ERR_UNEXPECTED;
          store_row_factory_.free_row_store(reuse_row);
          LOG_WARN("failed to push back row", K(ret));
        }
      }
      updated = true;
    } else {
      reuse_row = bucket_heap_->top();
      const bool less = (*comp_)(bucket_head_row, reuse_row);
      if (OB_SUCCESS != comp_->ret_) {
        ret = comp_->ret_;
        LOG_WARN("failed to compare", K(ret));
      } else if (less) {
        if (OB_FAIL(
                store_row_factory_.copy_to_row(bucket_head_row, reuse_row))) {
          LOG_WARN("failed to generate new row", K(ret));
        } else if (OB_FAIL(bucket_heap_->replace_top(reuse_row))) {
          LOG_WARN("failed to replace heap top element", K(ret));
        }
        updated = true;
      } else {
        updated = false;
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
void ObSortVecOpEagerFilter<Compare, Store_Row, has_addon>::reset() {
  if (nullptr != bucket_heap_) {
    for (int64_t i = 0; i < bucket_heap_->count(); i++) {
      store_row_factory_.free_row_store(bucket_heap_->at(i));
      bucket_heap_->at(i) = nullptr;
    }
    bucket_heap_->reset();
    bucket_heap_->~BucketHeap();
    allocator_.free(bucket_heap_);
    bucket_heap_ = nullptr;
  }
  if (nullptr != output_brs_.skip_) {
    allocator_.free(output_brs_.skip_);
    output_brs_.skip_ = nullptr;
  }
  bucket_size_ = 0;
  bucket_num_ = 0;
  comp_ = nullptr;
  is_inited_ = false;
}
} // namespace sql
} // namespace oceanbase