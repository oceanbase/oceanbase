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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_cache_lookup_iter.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASCacheLookupIter::IndexProjRowStore::init(common::ObIAllocator &allocator,
                                                  const common::ObIArray<ObExpr*> &exprs,
                                                  ObEvalCtx *eval_ctx,
                                                  int64_t max_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr for init index proj row store", K(ret));
  } else if (OB_ISNULL(store_rows_ =
      static_cast<ObChunkDatumStore::LastStoredRow*>(allocator.alloc(max_size * sizeof(ObChunkDatumStore::LastStoredRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(max_size), K(ret));
  } else if (FALSE_IT(index_scan_proj_exprs_.set_allocator(&allocator))) {
  } else if (OB_FAIL(index_scan_proj_exprs_.assign(exprs))) {
    LOG_WARN("failed to assign index scan proj exprs", K(max_size), K(ret));
  } else {
    eval_ctx_ = eval_ctx;
    max_size_ = max_size;
    for (int64_t i = 0; i < max_size_; i++) {
      new (store_rows_ + i) ObChunkDatumStore::LastStoredRow(allocator);
      store_rows_[i].reuse_ = true;
    }
  }

  return ret;
}

int ObDASCacheLookupIter::IndexProjRowStore::save(bool is_vectorized, int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index_scan_proj_exprs_.count() == 0)) {
    // do nothing
  } else if (OB_UNLIKELY(size + saved_size_ > max_size_) ||
             OB_ISNULL(store_rows_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error for save store rows", K(size), K(max_size_), K(store_rows_), K(ret));
  } else {
    if (is_vectorized) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(size);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
        batch_info_guard.set_batch_idx(i);
        if (OB_FAIL(store_rows_[i + saved_size_].save_store_row(index_scan_proj_exprs_, *eval_ctx_))) {
          LOG_WARN("cache lookup iter failed to store rows", K(ret));
        }
      }
    } else if (OB_FAIL(store_rows_[saved_size_].save_store_row(index_scan_proj_exprs_, *eval_ctx_))) {
      LOG_WARN("cache lookup iter failed to store rows", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    cur_idx_ = 0;
    saved_size_ += size;
  }

  return ret;
}

int ObDASCacheLookupIter::IndexProjRowStore::to_expr(int64_t size)
{
  int ret = OB_SUCCESS;
  if ((OB_UNLIKELY(index_scan_proj_exprs_.count() == 0))) {
    // do nothing
  } else if (OB_UNLIKELY(cur_idx_ == OB_INVALID_INDEX) ||
             OB_UNLIKELY(size + cur_idx_ > saved_size_) ||
             OB_ISNULL(store_rows_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error for convert store rows", K(size), K(max_size_), K(store_rows_), K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    batch_info_guard.set_batch_size(size);
    for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
      batch_info_guard.set_batch_idx(i);
      if (OB_FAIL(store_rows_[cur_idx_].store_row_->to_expr<true>(index_scan_proj_exprs_, *eval_ctx_))) {
        LOG_WARN("cache lookup iter failed to convert store row to expr", K(ret));
      } else {
        cur_idx_++;
      }
    }
  }
  return ret;
}

void ObDASCacheLookupIter::IndexProjRowStore::IndexProjRowStore::reuse()
{
  cur_idx_ = OB_INVALID_INDEX;
  saved_size_ = 0;
  iter_end_ = false;
}

void ObDASCacheLookupIter::IndexProjRowStore::IndexProjRowStore::reset()
{
  if (OB_NOT_NULL(store_rows_)) {
    for (int64_t i = 0; i < max_size_; i++) {
      store_rows_[i].ObChunkDatumStore::LastStoredRow::~LastStoredRow();
    }
    store_rows_ = nullptr;
  }
  index_scan_proj_exprs_.reset();
  eval_ctx_ = nullptr;
  max_size_ = 1;
  saved_size_ = 0;
  cur_idx_ = OB_INVALID_INDEX;
  iter_end_ = false;
}

int ObDASCacheLookupIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  ObDASCacheLookupIterParam &lookup_param = static_cast<ObDASCacheLookupIterParam&>(param);
  const int64_t default_batch_row_count = lookup_param.default_batch_row_count_;
  const int64_t simulate_batch_row_cnt = - EVENT_CALL(EventTable::EN_TABLE_LOOKUP_BATCH_ROW_COUNT);
  const bool use_simulate_batch_row_cnt = simulate_batch_row_cnt > 0 && simulate_batch_row_cnt < default_batch_row_count;

  lookup_param.default_batch_row_count_  = use_simulate_batch_row_cnt ? simulate_batch_row_cnt : default_batch_row_count;
  LOG_DEBUG("simulate lookup row batch count", K(simulate_batch_row_cnt), K(default_batch_row_count));

  if (OB_FAIL(ObDASLocalLookupIter::inner_init(param))) {
    LOG_WARN("failed to init das lookup iter", K(ret));
  } else if (lookup_param.index_scan_proj_exprs_.count() > 0 &&
             OB_FAIL(index_proj_rows_.init(store_allocator_,
                                           lookup_param.index_scan_proj_exprs_,
                                           lookup_param.eval_ctx_,
                                           lookup_param.default_batch_row_count_))) {
    LOG_WARN("failed to init index proj rows", K(ret));
  }

  return ret;
}

int ObDASCacheLookupIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASLocalLookupIter::inner_reuse())) {
    LOG_WARN("failed to reuse das lookup iter", K(ret));
  } else {
    index_proj_rows_.reuse();
  }
  return ret;
}

int ObDASCacheLookupIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASLocalLookupIter::inner_release())) {
    LOG_WARN("failed to release das lookup iter", K(ret));
  } else {
    index_proj_rows_.reset();
    store_allocator_.reset();
  }
  return ret;
}

void ObDASCacheLookupIter::reset_lookup_state()
{
  ObDASLookupIter::reset_lookup_state();
  index_proj_rows_.reuse();
}

int ObDASCacheLookupIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_next_row = false;
  bool need_index_proj = index_proj_rows_.index_scan_proj_exprs_.count() > 0;
  OB_ASSERT(index_proj_rows_.max_size_ == default_batch_row_count_);

  do {
    switch (state_) {
      case INDEX_SCAN: {
        reset_lookup_state();
        while (OB_SUCC(ret) && !index_end_ && lookup_rowkey_cnt_ < default_batch_row_count_) {
          index_table_iter_->clear_evaluated_flag();
          if (OB_FAIL(index_table_iter_->get_next_row())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next row from index table", K(ret));
            } else {
              index_end_ = true;
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(add_rowkey())) {
            LOG_WARN("failed to add row key", K(ret));
          } else if (need_index_proj && OB_FAIL(index_proj_rows_.save(false, 1))) { // if need to project, save it
              LOG_WARN("save index proj rows failed", K(ret));
          } else {
            ++lookup_rowkey_cnt_;
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_LIKELY(lookup_rowkey_cnt_ > 0)) {
            state_ = DO_LOOKUP;
          } else {
            state_ = FINISHED;
          }
        }
        break;
      }

      case DO_LOOKUP: {
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("failed to do index lookup", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }

      case OUTPUT_ROWS: {
        data_table_iter_->clear_evaluated_flag();
        if (OB_FAIL(data_table_iter_->get_next_row())) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
            if (OB_FAIL(check_index_lookup())) {
              LOG_WARN("failed to check table lookup", K(ret));
            } else {
              state_ = INDEX_SCAN;
            }
          } else {
            LOG_WARN("failed to get next row from data table", K(ret));
          }
        } else {
          got_next_row = true;
          ++lookup_row_cnt_;
          if (got_next_row && index_proj_rows_.have_data() &&
              OB_FAIL(index_proj_rows_.to_expr(1))) {
            LOG_WARN("failed to convert store row to expr", K(ret));
          }
        }
        break;
      }

      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected lookup state", K_(state));
      }
    }
  } while (!got_next_row && OB_SUCC(ret));

  return ret;
}

int ObDASCacheLookupIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool get_next_rows = false;
  bool need_index_proj = index_proj_rows_.index_scan_proj_exprs_.count() > 0;

  do {
    switch (state_) {
      case INDEX_SCAN: {
        reset_lookup_state();
        int64_t storage_count = 0;
        int64_t index_capacity = 0;
        // TODO: @zyx439997 support the outputs of index scan as the project columns by the deep copy {
        // }
        while (OB_SUCC(ret) && !index_end_ && lookup_rowkey_cnt_ < default_batch_row_count_) {
          storage_count = 0;
          index_capacity = std::min(capacity, std::min(max_size_, default_batch_row_count_ - lookup_rowkey_cnt_));
          index_table_iter_->clear_evaluated_flag();
          if (OB_FAIL(index_table_iter_->get_next_rows(storage_count, index_capacity))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next rows from index table", K(ret));
            } else {
              if (storage_count == 0) {
                index_end_ = true;
              }
              ret = OB_SUCCESS;
            }
          }
          if (OB_SUCC(ret) && storage_count > 0) {
            if (OB_FAIL(add_rowkeys(storage_count))) {
              LOG_WARN("failed to add row keys", K(ret));
            } else if (need_index_proj && OB_FAIL(index_proj_rows_.save(true, storage_count))) { // if need to project, save it
              LOG_WARN("save index proj rows failed", K(ret));
            } else {
              lookup_rowkey_cnt_ += storage_count;
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_LIKELY(lookup_rowkey_cnt_ > 0)) {
            state_ = DO_LOOKUP;
          } else {
            state_ = FINISHED;
          }
        }
        break;
      }

      case DO_LOOKUP: {
        if (OB_FAIL(do_index_lookup())) {
          LOG_WARN("failed to do index lookup", K(ret));
        } else {
          state_ = OUTPUT_ROWS;
        }
        break;
      }

      case OUTPUT_ROWS: {
        count = 0;
        data_table_iter_->clear_evaluated_flag();
        if (OB_FAIL(data_table_iter_->get_next_rows(count, capacity))) {
          if (OB_LIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
            if (count > 0) {
              lookup_row_cnt_ += count;
              get_next_rows = true;
            } else {
              if (OB_FAIL(check_index_lookup())) {
                LOG_WARN("failed to check table lookup", K(ret));
              } else {
                state_ = INDEX_SCAN;
              }
            }
          } else {
            LOG_WARN("failed to get next rows from data table", K(ret));
          }
        } else {
          lookup_row_cnt_ += count;
          get_next_rows = true;
        }

        if (OB_FAIL(ret)) {
        } else if (get_next_rows && index_proj_rows_.have_data() &&
                   OB_FAIL(index_proj_rows_.to_expr(count))) {
          LOG_WARN("failed to convert store row to expr", K(ret));
        }
        break;
      }

      case FINISHED: {
        ret = OB_ITER_END;
        break;
      }
    }
  } while (!get_next_rows && OB_SUCC(ret));

  return ret;
}

}  // namespace sql
}  // namespace oceanbase