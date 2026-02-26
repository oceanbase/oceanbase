/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_sparse_lookup_iter.h"

namespace oceanbase
{
namespace storage
{

ObSRLookupIter::ObSRLookupIter()
  : ObISparseRetrievalMergeIter(),
    iter_allocator_(nullptr),
    iter_param_(nullptr),
    merge_iter_(nullptr),
    cache_capacity_(0),
    rangekey_size_(0),
    cached_domain_ids_(),
    cmp_func_(nullptr),
    set_datum_func_(nullptr)
{}

int ObSRLookupIter::init(
    ObSparseRetrievalMergeParam &iter_param,
    ObISparseRetrievalMergeIter &merge_iter,
    ObIAllocator &iter_allocator,
    const int64_t cache_capacity)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else {
    iter_allocator_ = &iter_allocator;
    iter_param_ = &iter_param;
    merge_iter_ = &merge_iter;
    cache_capacity_ = cache_capacity;
    if (OB_UNLIKELY(cache_capacity_ < iter_param_->max_batch_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cache capacity", K(ret), K_(cache_capacity), K_(iter_param_->max_batch_size));
    } else if (FALSE_IT(cached_domain_ids_.set_allocator(iter_allocator_))) {
    } else if (OB_FAIL(cached_domain_ids_.init(cache_capacity_))) {
      LOG_WARN("failed to init iter domain ids", K(ret));
    } else if (OB_FAIL(cached_domain_ids_.prepare_allocate(cache_capacity_))) {
      LOG_WARN("failed to prepare allocate iter domain ids", K(ret));
    } else if (OB_FAIL(inner_init())) {
      LOG_WARN("failed to inner init lookup iter", K(ret));
    } else {
      ObDatumMeta id_meta = iter_param_->id_proj_expr_->datum_meta_;
      sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(id_meta.type_, id_meta.cs_type_);
      cmp_func_ = lib::is_oracle_mode() ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
      if (iter_param_->id_proj_expr_->datum_meta_.type_ == common::ObUInt64Type) {
        set_datum_func_ = ObISparseRetrievalMergeIter::set_datum_int;
      } else {
        set_datum_func_ = ObISparseRetrievalMergeIter::set_datum_shallow;
      }
      is_inited_ = true;
    }
  }
  LOG_DEBUG("init sr lookup iter", K_(cache_capacity));
  return ret;
}

void ObSRLookupIter::reset()
{
  if (OB_NOT_NULL(merge_iter_)) {
    merge_iter_->reset();
  }
  cached_domain_ids_.reset();
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  is_inited_ = false;
  LOG_DEBUG("reset sr lookup iter");
}

void ObSRLookupIter::reuse(const bool switch_tablet)
{
  if (OB_NOT_NULL(merge_iter_)) {
    merge_iter_->reuse(switch_tablet);
  }
  rangekey_size_ = 0;
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  LOG_DEBUG("reuse sr lookup iter");
}

int ObSRLookupIter::get_next_row()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(1 != rangekey_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rangekey size", K(ret), K_(rangekey_size));
  } else if (OB_FAIL(merge_iter_->get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row from merge iter", K(ret));
    } else if (0 != output_row_cnt_) {
      // do nothing
    } else {
      ObEvalCtx *eval_ctx = iter_param_->eval_ctx_;
      ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
      guard.set_batch_idx(0);
      ObDatum &id_proj_datum = iter_param_->id_proj_expr_->locate_datum_for_write(*eval_ctx);
      ObDatum &relevance_proj_datum = iter_param_->relevance_proj_expr_->locate_datum_for_write(*eval_ctx);
      set_datum_func_(id_proj_datum, cached_domain_ids_.at(0));
      relevance_proj_datum.set_double(0.0);
      ++output_row_cnt_;
      ret = OB_SUCCESS;
    }
  } else {
    ++output_row_cnt_;
  }
  LOG_DEBUG("get next row from sr lookup iter", K(ret), K_(output_row_cnt));
  return ret;
}

int ObSRLookupIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(0 == capacity)) {
    count = 0;
  } else if (0 == output_row_cnt_ && OB_FAIL(load_results())) {
    LOG_WARN("failed to load results", K(ret), K(capacity), K(count));
  } else if (OB_FAIL(project_results(capacity, count))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to project results", K(ret), K(capacity), K(count));
    }
  }
  LOG_DEBUG("get next rows from sr lookup iter", K(ret), K_(output_row_cnt), K(capacity), K(count));
  return ret;
}

ObSRSortedLookupIter::ObSRSortedLookupIter()
  : ObSRLookupIter(),
    reverse_hints_(),
    cached_relevances_()
{}

int ObSRSortedLookupIter::inner_init()
{
  int ret = OB_SUCCESS;
  reverse_hints_.set_allocator(iter_allocator_);
  if (OB_FAIL(reverse_hints_.init(cache_capacity_))) {
    LOG_WARN("failed to init hints array", K(ret));
  } else if (OB_FAIL(reverse_hints_.prepare_allocate(cache_capacity_))) {
    LOG_WARN("failed to prepare allocate hints array", K(ret));
  } else if (FALSE_IT(cached_relevances_.set_allocator(iter_allocator_))) {
  } else if (OB_FAIL(cached_relevances_.init(cache_capacity_))) {
    LOG_WARN("failed to init relevances array", K(ret));
  } else if (OB_FAIL(cached_relevances_.prepare_allocate(cache_capacity_))) {
    LOG_WARN("failed to prepare allocate relevances array", K(ret));
  }
  return ret;
}

void ObSRSortedLookupIter::reset()
{
  reverse_hints_.reset();
  cached_relevances_.reset();
  ObSRLookupIter::reset();
}

int ObSRSortedLookupIter::load_results()
{
  int ret = OB_SUCCESS;
  int cur_idx = 0;
  bool iter_end = false;
  while (OB_SUCC(ret) && !iter_end && cur_idx < rangekey_size_) {
    int64_t sub_count = 0;
    if (OB_FAIL(merge_iter_->get_next_rows(rangekey_size_ - cur_idx, sub_count))) {
      iter_end = true;
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next rows from merge iter", K(ret));
      } else if (sub_count > 0) {
        ret = OB_SUCCESS;
      }
    }
    LOG_DEBUG("sr lookup iter loading results", K(ret), K_(rangekey_size), K(cur_idx), K(sub_count));
    ObEvalCtx *eval_ctx = iter_param_->eval_ctx_;
    const ObDatumVector &id_datums = iter_param_->id_proj_expr_->locate_expr_datumvector(*eval_ctx);
    const ObDatumVector &relevance_datums = iter_param_->relevance_proj_expr_->locate_expr_datumvector(*eval_ctx);
    int cmp_result = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_count; ) {
      if (OB_UNLIKELY(cur_idx >= rangekey_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cur idx", K(ret), K(cur_idx), K_(rangekey_size));
      } else if (OB_FAIL(cmp_func_(cached_domain_ids_[cur_idx].get_datum(), *id_datums.at(i), cmp_result))) {
        LOG_WARN("failed to compare id datums", K(ret));
      } else if (0 == cmp_result) {
        cached_relevances_[cur_idx] = relevance_datums.at(i)->get_double();
        ++cur_idx;
        ++i;
      } else if (cmp_result < 0) {
        cached_relevances_[cur_idx] = 0.0;
        ++cur_idx;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected comparison result", K(ret), K(cmp_result));
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSRSortedLookupIter::project_results(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  ObEvalCtx *eval_ctx = iter_param_->eval_ctx_;
  ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
  ObExpr *id_proj_expr = iter_param_->id_proj_expr_;
  ObExpr *relevance_proj_expr = iter_param_->relevance_proj_expr_;
  sql::ObBitVector &id_evaluated_flags = id_proj_expr->get_evaluated_flags(*eval_ctx);
  sql::ObBitVector &relevance_evaluated_flags = relevance_proj_expr->get_evaluated_flags(*eval_ctx);
  while (count < capacity && output_row_cnt_ < rangekey_size_) {
    guard.set_batch_idx(count);
    ObDatum &id_proj_datum = id_proj_expr->locate_datum_for_write(*eval_ctx);
    set_datum_func_(id_proj_datum, cached_domain_ids_[reverse_hints_[output_row_cnt_]]);
    id_evaluated_flags.set(count);
    ObDatum &relevance_proj_datum = relevance_proj_expr->locate_datum_for_write(*eval_ctx);
    relevance_proj_datum.set_double(cached_relevances_[reverse_hints_[output_row_cnt_]]);
    relevance_evaluated_flags.set(count);
    ++count;
    ++output_row_cnt_;
  }
  if (output_row_cnt_ == rangekey_size_) {
    ret = OB_ITER_END;
  }
  id_proj_expr->set_evaluated_projected(*eval_ctx);
  relevance_proj_expr->set_evaluated_projected(*eval_ctx);
  return ret;
}

int ObSRSortedLookupIter::set_hints(const common::ObIArray<std::pair<ObDocIdExt, int>> &virtual_rangekeys, const int64_t size)
{
  int ret = OB_SUCCESS;
  rangekey_size_ = size;
  if (OB_UNLIKELY(rangekey_size_ > cache_capacity_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("unexpected rangekey size", K(ret), K_(rangekey_size), K_(cache_capacity));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      cached_domain_ids_.at(i) = virtual_rangekeys.at(i).first;
      cached_relevances_.at(i) = 0.0;
      if (OB_UNLIKELY(virtual_rangekeys.at(i).second >= cache_capacity_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected rangekey", K(ret), K(virtual_rangekeys.at(i).second), K_(cache_capacity));
      } else {
        reverse_hints_[virtual_rangekeys.at(i).second] = i;
      }
    }
  }
  LOG_DEBUG("set hints of sr lookup iter", K(ret), K_(cache_capacity), K_(rangekey_size));
  return ret;
}

ObSRHashLookupIter::ObSRHashLookupIter()
  : ObSRLookupIter(),
    hash_map_()
{}

int ObSRHashLookupIter::inner_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hash_map_.create(cache_capacity_, common::ObMemAttr(MTL_ID(), "SRTaaTMap")))) {
    LOG_WARN("failed to create hash map");
  }
  return ret;
}

void ObSRHashLookupIter::reset()
{
  hash_map_.destroy();
  ObSRLookupIter::reset();
}

int ObSRHashLookupIter::load_results()
{
  int ret = OB_SUCCESS;
  int cur_idx = 0;
  bool iter_end = false;
  while (OB_SUCC(ret) && !iter_end) {
    int64_t sub_count = 0;
    if (OB_FAIL(merge_iter_->get_next_rows(rangekey_size_ - cur_idx, sub_count))) {
      iter_end = true;
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next rows from merge iter", K(ret));
      } else if (sub_count > 0) {
        ret = OB_SUCCESS;
      }
    }
    LOG_DEBUG("sr lookup iter loading results", K(ret), K_(rangekey_size), K(cur_idx), K(sub_count));
    ObEvalCtx *eval_ctx = iter_param_->eval_ctx_;
    const ObDatumVector &id_datums = iter_param_->id_proj_expr_->locate_expr_datumvector(*eval_ctx);
    const ObDatumVector &relevance_datums = iter_param_->relevance_proj_expr_->locate_expr_datumvector(*eval_ctx);
    ObDocIdExt id;
    double relevance = 0.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_count; ++i) {
      if (OB_UNLIKELY(cur_idx >= rangekey_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cur idx", K(ret), K(cur_idx), K_(rangekey_size));
      } else if (OB_FAIL(id.from_datum(*id_datums.at(i)))) {
        LOG_WARN("failed to get id from datum", K(ret));
      } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != (ret = hash_map_.get_refactored(id, relevance)))) {
        ret = COVER_SUCC(OB_ERR_UNEXPECTED);
        LOG_WARN("unexpected repeated domain id", K(ret), K(id), K(relevance));
      } else if (OB_FAIL(hash_map_.set_refactored(id, relevance_datums.at(i)->get_double(), 0))) {
        LOG_WARN("failed to set relevance in hash map", K(ret));
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSRHashLookupIter::project_results(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  ObEvalCtx *eval_ctx = iter_param_->eval_ctx_;
  ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
  ObExpr *id_proj_expr = iter_param_->id_proj_expr_;
  ObExpr *relevance_proj_expr = iter_param_->relevance_proj_expr_;
  sql::ObBitVector &id_evaluated_flags = id_proj_expr->get_evaluated_flags(*eval_ctx);
  sql::ObBitVector &relevance_evaluated_flags = relevance_proj_expr->get_evaluated_flags(*eval_ctx);
  while (OB_SUCC(ret) && count < capacity && output_row_cnt_ < rangekey_size_) {
    guard.set_batch_idx(count);
    ObDatum &id_proj_datum = id_proj_expr->locate_datum_for_write(*eval_ctx);
    set_datum_func_(id_proj_datum, cached_domain_ids_[output_row_cnt_]);
    id_evaluated_flags.set(count);
    ObDatum &relevance_proj_datum = relevance_proj_expr->locate_datum_for_write(*eval_ctx);
    double relevance = 0.0;
    if (OB_FAIL(hash_map_.get_refactored(cached_domain_ids_[output_row_cnt_], relevance))) {
      relevance = 0.0;
      if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get relevance from hash map", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      relevance_proj_datum.set_double(relevance);
      relevance_evaluated_flags.set(count);
      ++count;
      ++output_row_cnt_;
    }
  }
  if (OB_SUCC(ret) && output_row_cnt_ == rangekey_size_) {
    ret = OB_ITER_END;
  }
  id_proj_expr->set_evaluated_projected(*eval_ctx);
  relevance_proj_expr->set_evaluated_projected(*eval_ctx);
  return ret;
}

int ObSRHashLookupIter::set_hints(const common::ObIArray<std::pair<ObDocIdExt, int>> &virtual_rangekeys, const int64_t size)
{
  int ret = OB_SUCCESS;
  rangekey_size_ = size;
  if (OB_UNLIKELY(rangekey_size_ > cache_capacity_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("unexpected rangekey size", K(ret), K_(rangekey_size), K_(cache_capacity));
  } else {
    hash_map_.clear();
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      if (OB_UNLIKELY(virtual_rangekeys.at(i).second >= cache_capacity_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected rangekey", K(ret), K(virtual_rangekeys.at(i).second), K_(cache_capacity));
      } else {
        cached_domain_ids_[virtual_rangekeys.at(i).second] = virtual_rangekeys.at(i).first;
      }
    }
  }
  LOG_DEBUG("set hints of sr lookup iter", K(ret), K_(cache_capacity), K_(rangekey_size));
  return ret;
}

} // namespace storage
} // namespace oceanbase