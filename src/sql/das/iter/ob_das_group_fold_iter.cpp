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
#include "sql/das/iter/ob_das_group_fold_iter.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace sql
{

int ObGroupResultSaveRows::init(const common::ObIArray<ObExpr*> &exprs,
                                ObEvalCtx &eval_ctx,
                                int64_t max_size,
                                int64_t group_id_idx,
                                bool need_check_output_datum,
                                common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    inited_ = true;
    need_check_output_datum_ = need_check_output_datum;
    exprs_ = &exprs;
    eval_ctx_ = &eval_ctx;
    max_size_ = max_size;
    group_id_idx_ = group_id_idx;
    if (OB_ISNULL(store_rows_ =
        static_cast<LastDASStoreRow*>(allocator.alloc(max_size * sizeof(LastDASStoreRow))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(max_size), K(ret));
    } else {
      for (int64_t i = 0; i < max_size_; i++) {
        new (store_rows_ + i) LastDASStoreRow(allocator);
        store_rows_[i].reuse_ = true;
      }
    }
  }

  return ret;
}

int ObGroupResultSaveRows::save(bool is_vectorized, int64_t start_pos, int64_t size)
{
  int ret = OB_SUCCESS;
  if (start_pos + size > max_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, exceeds max size", K(ret), K(start_pos), K(size), K_(max_size));
  } else {
    if (is_vectorized) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(start_pos + size);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
        batch_info_guard.set_batch_idx(start_pos + i);
        OZ(store_rows_[i].save_store_row(*exprs_, *eval_ctx_));
      }
    } else {
      OZ(store_rows_[0].save_store_row(*exprs_, *eval_ctx_));
    }
    start_pos_ = 0;
    saved_size_ = size;
  }

  return ret;
}

int ObGroupResultSaveRows::to_expr(bool is_vectorized, int64_t start_pos, int64_t size)
{
  int ret = OB_SUCCESS;
  if (is_vectorized) {
    if (start_pos + size > saved_size_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(start_pos), K(size), K_(saved_size), K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(size);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
        batch_info_guard.set_batch_idx(i);
        OZ(store_rows_[start_pos + i].store_row_->to_expr<true>(*exprs_, *eval_ctx_));
      }
    }
  } else {
    OZ(store_rows_[0].store_row_->to_expr<false>(*exprs_, *eval_ctx_));
  }

  return ret;
}

int64_t ObGroupResultSaveRows::cur_group_idx()
{
  return start_pos_ >= saved_size_ ? OB_INVALID_INDEX :
      ObNewRange::get_group_idx(store_rows_[start_pos_].store_row_->cells()[group_id_idx_].get_int());
}


void ObGroupResultSaveRows::reuse()
{
  start_pos_ = 0;
  saved_size_ = 0;
}

void ObGroupResultSaveRows::reset()
{
  inited_ = false;
  exprs_ = nullptr;
  eval_ctx_ = nullptr;
  saved_size_ = 0;
  max_size_ = 1;
  start_pos_ = 0;
  group_id_idx_ = 0;
  need_check_output_datum_ = false;
  if (OB_NOT_NULL(store_rows_)) {
    for (int64_t i = 0; i < max_size_; i++) {
      store_rows_[i].~LastDASStoreRow();
    }
    store_rows_ = nullptr;
  }
}

int ObDASGroupFoldIter::set_scan_group(int64_t group_id)
{
  int ret = OB_SUCCESS;
  // TODO bingfan: add defensive check
  if (OB_INVALID_INDEX == group_id) {
    cur_group_idx_ += 1;
  } else {
    cur_group_idx_ = group_id;
  }
  if (group_save_rows_.need_check_output_datum_) {
    reset_expr_datum_ptr();
  }
  if (cur_group_idx_ >= group_size_) {
    ret = OB_ITER_END;
  }
  LOG_TRACE("set group id for fold iter", K(cur_group_idx_), K(group_id), K(group_size_), K(lbt()));
  LOG_DEBUG("set scan group", K(ret), K(group_id), K(*this));
  return ret;
}

void ObDASGroupFoldIter::init_group_range(int64_t cur_group_idx, int64_t group_size)
{
  available_group_idx_ = MIN_GROUP_INDEX;
  cur_group_idx_ = cur_group_idx;
  group_size_ = group_size;
}

void ObDASGroupFoldIter::reset_expr_datum_ptr()
{
  if (OB_NOT_NULL(group_save_rows_.exprs_)) {
    FOREACH_CNT(e, *group_save_rows_.exprs_)
    {
      (*e)->locate_datums_for_update(*group_save_rows_.eval_ctx_, group_save_rows_.max_size_);
      ObEvalInfo &info = (*e)->get_eval_info(*group_save_rows_.eval_ctx_);
      info.point_to_frame_ = true;
    }
  }
}

int ObDASGroupFoldIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (param.type_ != ObDASIterType::DAS_ITER_GROUP_FOLD || OB_ISNULL(param.group_id_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param", K(param), K(ret));
  } else {
    ObDASGroupFoldIterParam &group_fold_param = static_cast<ObDASGroupFoldIterParam&>(param);
    cur_group_idx_ = 0;
    available_group_idx_ = MIN_GROUP_INDEX;
    group_size_ = 0;
    need_check_output_datum_ = group_fold_param.need_check_output_datum_;
    iter_tree_ = group_fold_param.iter_tree_;
    iter_alloc_ = new (iter_alloc_buf_) common::ObArenaAllocator();
    iter_alloc_->set_attr(ObMemAttr(MTL_ID(), "ScanDASCtx"));

    /********* init group store rows *********/
    int64_t group_id_idx = OB_INVALID_INDEX;
    for (int64_t i = 0 ; i < output_->count(); i++) {
      if (output_->at(i) == group_id_expr_) {
        group_id_idx = i;
      }
    }
    if (group_id_idx == OB_INVALID_INDEX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get group id idx", K(ret));
    } else if (OB_FAIL(group_save_rows_.init(*output_,
                                             *eval_ctx_,
                                             max_size_,
                                             group_id_idx,
                                             need_check_output_datum_,
                                             *iter_alloc_))) {
      LOG_WARN("failed to init group save rows", K(ret));
    }
  }

  return ret;
}

int ObDASGroupFoldIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  cur_group_idx_ = 0;
  available_group_idx_ = MIN_GROUP_INDEX;
  group_size_ = 0;
  group_save_rows_.reuse();
  return ret;
}

int ObDASGroupFoldIter::inner_release()
{
  int ret = OB_SUCCESS;
  cur_group_idx_ = 0;
  available_group_idx_ = MIN_GROUP_INDEX;
  group_size_ = 0;
  group_save_rows_.reset();
  iter_tree_ = nullptr;
  if (OB_NOT_NULL(iter_alloc_)) {
    iter_alloc_->reset();
    iter_alloc_->~ObArenaAllocator();
    iter_alloc_ = nullptr;
  }
  return ret;
}

int ObDASGroupFoldIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t storage_count = 0;
  int64_t ret_count = 0;
  int64_t group_idx = MIN_GROUP_INDEX;
  LOG_DEBUG("das group fold iter get next rows begin", K_(available_group_idx), K_(cur_group_idx));

  if (available_group_idx_ > cur_group_idx_) {
    ret = OB_ITER_END;
    LOG_TRACE("available_group_idx > cur_group_idx, no available rows", K_(available_group_idx), K_(cur_group_idx));
  } else {
    while (MIN_GROUP_INDEX != available_group_idx_ && available_group_idx_ < cur_group_idx_) {
      group_save_rows_.next_start_pos();
      available_group_idx_ = group_save_rows_.cur_group_idx();
      if (OB_INVALID_INDEX == available_group_idx_) {
        // row_store_ has been consumed and new data needs to be fetched from the storage layer.
        available_group_idx_ = MIN_GROUP_INDEX;
      }
    }
  }

  // fetch new data from storage layer.
  while (OB_SUCC(ret) && MIN_GROUP_INDEX == available_group_idx_) {
    if (OB_FAIL(iter_tree_->get_next_rows(storage_count, capacity))) {
      if (OB_ITER_END == ret) {
        if (storage_count > 0) {
          ret = OB_SUCCESS;
        } else {
          LOG_DEBUG("underlying iter tree reached iter end", K_(available_group_idx), K_(cur_group_idx));
          // subsequent calls to get next rows will no longer be able to return rows.
          available_group_idx_ = INT64_MAX;
        }
      } else {
        LOG_WARN("underlying iter tree failed to get next rows", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      const ObBitVector *skip = nullptr;
      PRINT_VECTORIZED_ROWS(SQL, DEBUG, *eval_ctx_, *output_, storage_count, skip);
      ObDatum *group_idx_batch = group_id_expr_->locate_batch_datums(*group_save_rows_.eval_ctx_);
      for (int64_t i = 0; OB_SUCC(ret) && i < storage_count; i++) {
        group_idx = ObNewRange::get_group_idx(group_idx_batch[i].get_int());
        if (group_idx >= cur_group_idx_) {
          if (OB_FAIL(group_save_rows_.save(true, i, storage_count - i))) {
            LOG_WARN("das group fold iter failed to save batch result", K(ret));
          } else {
            available_group_idx_ = group_idx;
          }
          break;
        }
      }
    }
  } // while end

  if (OB_SUCC(ret)) {
    if (available_group_idx_ == cur_group_idx_) { // there are rows available in row_store_.
      int64_t start_pos = group_save_rows_.get_start_pos();
      while (cur_group_idx_ == available_group_idx_) {
        group_idx = group_save_rows_.cur_group_idx();
        if (cur_group_idx_ == group_idx) {
          group_save_rows_.next_start_pos();
          ret_count++;
        } else {
          available_group_idx_ = group_idx;
          if (OB_INVALID_INDEX == available_group_idx_) {
            available_group_idx_ = MIN_GROUP_INDEX;
          }
        }
      } // while end

      if (ret_count > 0) {
        OZ(group_save_rows_.to_expr(true, start_pos, ret_count));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("das group fold iter do not get any rows", K(ret_count), K_(group_save_rows),
            K_(cur_group_idx), K_(available_group_idx), K(ret));
      }
      // the group_idx of the data in row_store is already larger than cur_group_idx,
      // which means there is no more data for this group.
      if (OB_SUCC(ret) && MIN_GROUP_INDEX != available_group_idx_ && cur_group_idx_ != available_group_idx_) {
        ret = OB_ITER_END;
      }
    } else {
      OB_ASSERT(available_group_idx_ > cur_group_idx_ && available_group_idx_ != INT64_MAX);
      LOG_TRACE("all new rows from storage layer have greater group idx", K_(available_group_idx), K_(cur_group_idx));
      ret = OB_ITER_END;
    }
  }
  count = ret_count;

  LOG_DEBUG("das group fold iter get next rows end", K(ret_count), K(storage_count), K(*this));
  return ret;
}

int ObDASGroupFoldIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (available_group_idx_ > cur_group_idx_) {
    ret = OB_ITER_END;
    LOG_TRACE("available_group_idx > cur_group_idx, no available rows", K_(available_group_idx), K_(cur_group_idx));
  } else if (available_group_idx_ == cur_group_idx_) {
    OZ(group_save_rows_.to_expr(false, 0, 1));
    available_group_idx_ = MIN_GROUP_INDEX;
  } else {
    // fetch new data from storage layer.
    ObDatum *group_idx = NULL;
    while (OB_SUCC(ret) && available_group_idx_ < cur_group_idx_) {
      if (OB_FAIL(iter_tree_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("das group fold iter failed to get next row", K(ret), K_(available_group_idx), K_(cur_group_idx));
        } else {
          available_group_idx_ = INT64_MAX;
        }
      } else if (OB_FAIL(group_id_expr_->eval(*group_save_rows_.eval_ctx_, group_idx))) {
        LOG_WARN("failed to eval group id", K(ret));
      } else {
        available_group_idx_ = ObNewRange::get_group_idx(group_idx->get_int());
      }
    } // while end

    if (OB_SUCC(ret)) {
      if (available_group_idx_ == cur_group_idx_) {
        // return result
        available_group_idx_ = MIN_GROUP_INDEX;
      } else {
        if (OB_FAIL(group_save_rows_.save(false, 0, 1))) {
          LOG_WARN("failed to save last row", K(ret));
        } else {
          ret = OB_ITER_END;
        }
      }
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
