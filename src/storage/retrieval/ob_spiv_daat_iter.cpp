/**
 * Copyright (c) 2025 OceanBase
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

#include "ob_spiv_daat_iter.h"

namespace oceanbase
{
namespace storage
{

// ------------------ ObSPIVDaaTIter implement ------------------
int ObSPIVDaaTIter::init(const ObSPIVDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_init(param))) {
    LOG_WARN("failed to init spiv daat iter");
  }
  return ret;
}

ObSPIVDaaTIter::~ObSPIVDaaTIter()
{
  if (OB_NOT_NULL(sort_heap_) && OB_NOT_NULL(iter_allocator_)) {
    sort_heap_->~ObSPIVSortHeap();
    iter_allocator_->free(sort_heap_);
    sort_heap_ = nullptr;
  }
}

int ObSPIVDaaTIter::inner_init(const ObSPIVDaaTParam &param)
{
  return OB_NOT_IMPLEMENT;
}

int ObSPIVDaaTIter::pre_process()
{
  return OB_SUCCESS;
}

int ObSPIVDaaTIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(inner_get_next_rows(capacity, count))) {
    if(OB_UNLIKELY(ret != OB_ITER_END)) {
      LOG_WARN("failed to inner get next rows", K(ret));
    }
  }
  return ret;
}

int ObSPIVDaaTIter::get_next_row()
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(inner_get_next_row())) {
    if(OB_UNLIKELY(ret != OB_ITER_END)) {
      LOG_WARN("failed to inner get next row", K(ret));
    }
  }
  return ret;
}

int ObSPIVDaaTIter::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObSPIVDaaTIter::inner_get_next_rows(const int64_t capacity, int64_t &count)
{
  return OB_NOT_IMPLEMENT;
}

int ObSPIVDaaTIter::process()
{
  return OB_NOT_IMPLEMENT;
}

int ObSPIVDaaTIter::set_valid_docid_set(const common::hash::ObHashSet<ObDocIdExt> &valid_docid_set)
{
  int ret = OB_SUCCESS;
  valid_docid_set_.clear();
  int64_t expected_size = valid_docid_set.size();
  if (valid_docid_set.empty()){
  } else if (OB_FAIL(valid_docid_set_.create(expected_size, ObMemAttr(MTL_ID(), "ValidDocidSet")))) {
    LOG_WARN("failed to create valid docid set", K(ret), K(expected_size));
  } else {
    for (common::hash::ObHashSet<ObDocIdExt>::const_iterator iter = valid_docid_set.begin(); OB_SUCC(ret) && iter != valid_docid_set.end(); ++iter) {
      if (OB_FAIL(valid_docid_set_.set_refactored(iter->first))) {
        LOG_WARN("failed to insert docid to valid docid set", K(ret), K(iter->first));
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to set valid docid set", K(ret));
    valid_docid_set_.clear();
  }
  return ret;
}

void ObSPIVDaaTIter::reset()
{
  valid_docid_set_.clear();
  result_docids_.reset();
  result_docids_curr_iter_ = OB_INVALID_INDEX_INT64;
  ObSRDaaTIterImpl::reset();
  inner_reset();
}

void ObSPIVDaaTIter::reuse(const bool switch_tablet)
{
  valid_docid_set_.reuse();
  result_docids_.reuse();
  result_docids_curr_iter_ = OB_INVALID_INDEX_INT64;
  ObSRDaaTIterImpl::reuse();
  inner_reuse();
}

// ------------------ ObSPIVDaaTNaiveIter implement ------------------
int ObSPIVDaaTNaiveIter::inner_init(const ObSPIVDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null base param", K(ret));
  } else if (OB_FAIL(ObSRDaaTIterImpl::init(*param.base_param_, *param.dim_iters_, *param.allocator_, *param.relevance_collector_))) {
    LOG_WARN("failed to init spiv daat iter", K(ret));
  } else {
    is_pre_filter_ = param.is_pre_filter_;
    is_use_docid_ = param.is_use_docid_;
    if (is_use_docid_) {
      set_datum_func_ = set_datum_shallow;
    } else {
      set_datum_func_ = set_datum_int;
    }
    if (OB_ISNULL(iter_allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter_allocator_ is null", K(ret));
    } else if (OB_ISNULL(sort_heap_ = OB_NEWx(SPIVSortHeap,
                             iter_allocator_,
                             param.base_param_->topk_limit_,
                             *iter_allocator_,
                             docid_score_cmp_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to init sort heap", K(ret));
    }
  }
  return ret;
}

void ObSPIVDaaTNaiveIter::inner_reuse()
{
  for (int64_t i = 0; i < dim_iters_->count(); ++i) {
    static_cast<ObSPIVDaaTDimIter *>(dim_iters_->at(i))->reuse();
  }
}

void ObSPIVDaaTNaiveIter::inner_reset()
{
  for (int64_t i = 0; i < dim_iters_->count(); ++i) {
    static_cast<ObSPIVDaaTDimIter *>(dim_iters_->at(i))->reset();
  }
}

int ObSPIVDaaTNaiveIter::project_results(int64_t count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
    bool exist = false;
    if (is_pre_filter_) {
      int hash_ret = valid_docid_set_.exist_refactored(buffered_domain_ids_[i]);
      if (OB_HASH_EXIST == hash_ret) {
        exist = true;
      }
    }
    if (!is_pre_filter_ || exist) {
      // negative inner product
      if (OB_FAIL(sort_heap_->push(buffered_domain_ids_[i], -buffered_relevances_[i]))) {
        LOG_WARN("failed to push item to sort heap", K(ret));
      }
    }
  }
  return ret;
}

int ObSPIVDaaTNaiveIter::process()
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    int64_t count = 0;
    if (OB_FAIL(do_one_merge_round(count))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to do one merge round", K(ret), K(count));
      }
    }
    if ((OB_SUCC(ret) || OB_ITER_END == ret) && count > 0) {
      if (OB_FAIL(project_results(count))) {
        LOG_WARN("failed to project rows", K(ret));
      }
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    int heap_size = sort_heap_->count();
    if (heap_size == 0) {
      ret = OB_ITER_END;
    } else {
      ObDocIdExt docid;
      if (OB_FAIL(result_docids_.prepare_allocate(heap_size))) {
        LOG_WARN("failed to prepare allocate", K(ret));
      }
      for (int i = 0; OB_SUCC(ret) && i < heap_size; i++) {
        docid = sort_heap_->get_doc_id(sort_heap_->top().doc_idx_);
        // for negative inner product, using ascend sorting
        result_docids_[heap_size - 1 - i].from_datum(docid.get_datum());
        if (OB_FAIL(sort_heap_->pop())) {
          LOG_WARN("failed to pop docid", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    result_docids_curr_iter_ = 0;
  }
  return ret;
}

int ObSPIVDaaTNaiveIter::inner_get_next_row()
{
  int64_t count = 0;
  return inner_get_next_rows(1, count);
}

int ObSPIVDaaTNaiveIter::inner_get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (iter_param_->limit_param_->limit_ + iter_param_->limit_param_->offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_INVALID_INDEX_INT64 == result_docids_curr_iter_) {
    if (OB_FAIL(process())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to process", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_INDEX_INT64 == result_docids_curr_iter_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get heap iter", K(ret));
  } else if (result_docids_curr_iter_ == result_docids_.count()) {
    ret = OB_ITER_END;
  } else {
    count = OB_MIN(result_docids_.count() - result_docids_curr_iter_, capacity);
    ObExpr *id_proj_expr = iter_param_->id_proj_expr_;
    ObEvalCtx *eval_ctx = iter_param_->eval_ctx_;
    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
    sql::ObBitVector &id_evaluated_flags = id_proj_expr->get_evaluated_flags(*eval_ctx);
    for (int64_t i = 0; i < count; ++i) {
      guard.set_batch_idx(i);
      ObDatum &id_proj_datum = id_proj_expr->locate_datum_for_write(*eval_ctx);
      set_datum_func_(id_proj_datum, result_docids_.at(result_docids_curr_iter_++));
      id_evaluated_flags.set(i);
      id_proj_expr->set_evaluated_projected(*eval_ctx);
    }
  }

  return ret;
}

// ------------------ ObSPIVBMWIter implement ------------------
int ObSPIVBMWIter::init(const ObSPIVDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null base param", K(ret));
  } else if (OB_FAIL(ObSRBMWIterImpl::init(*param.base_param_, *param.dim_iters_, *param.allocator_, *param.relevance_collector_))) {
    LOG_WARN("failed to init spiv daat iter", K(ret));
  } else {
    is_pre_filter_ = param.is_pre_filter_;
    is_use_docid_ = param.is_use_docid_;
    if (is_use_docid_) {
      set_datum_func_ = set_datum_shallow;
    } else {
      set_datum_func_ = set_datum_int;
    }
  }
  return ret;
}

int ObSPIVBMWIter::set_valid_docid_set(const common::hash::ObHashSet<ObDocIdExt> &valid_docid_set)
{
  int ret = OB_SUCCESS;
  valid_docid_set_.clear();
  int64_t expected_size = valid_docid_set.size();
  if (valid_docid_set.empty()){
  } else if (OB_FAIL(valid_docid_set_.create(expected_size, ObMemAttr(MTL_ID(), "ValidDocidSet")))) {
    LOG_WARN("failed to create valid docid set", K(ret), K(expected_size));
  } else {
    for (common::hash::ObHashSet<ObDocIdExt>::const_iterator iter = valid_docid_set.begin(); OB_SUCC(ret) && iter != valid_docid_set.end(); ++iter) {
      if (OB_FAIL(valid_docid_set_.set_refactored(iter->first))) {
        LOG_WARN("failed to insert docid to valid docid set", K(ret), K(iter->first));
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to set valid docid set", K(ret));
    valid_docid_set_.clear();
  }
  return ret;
}

void ObSPIVBMWIter::reuse(const bool switch_tablet)
{
  valid_docid_set_.reuse();
  result_docids_.reuse();
  result_docids_curr_iter_ = OB_INVALID_INDEX_INT64;
  ObSRBMWIterImpl::reuse(switch_tablet);
  for (int64_t i = 0; i < dim_iters_->count(); ++i) {
    static_cast<ObSPIVBlockMaxDimIter *>(dim_iters_->at(i))->reuse();
  }
}

void ObSPIVBMWIter::reset()
{
  valid_docid_set_.clear();
  result_docids_.reset();
  result_docids_curr_iter_ = OB_INVALID_INDEX_INT64;
  ObSRBMWIterImpl::reset();
  for (int64_t i = 0; i < dim_iters_->count(); ++i) {
    static_cast<ObSPIVBlockMaxDimIter *>(dim_iters_->at(i))->reset();
  }
}

int ObSPIVBMWIter::get_next_row()
{
  int64_t count = 0;
  return get_next_rows(1, count);
}

int ObSPIVBMWIter::get_next_rows(const int64_t capacity, int64_t &count){
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(capacity));
  } else if (BMWStatus::FINISHED == status_) {
    // skip
  } else if (OB_FAIL(top_k_search())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to top k search", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (FAILEDx(project_rows_from_result_docids(capacity, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to project rows from top k heap", K(ret));
    }
  }
  return ret;
}

int ObSPIVBMWIter::reverse_top_k_heap()
{
  int ret = OB_SUCCESS;
  int heap_size = top_k_heap_.count();
  if (heap_size == 0) {
    ret = OB_ITER_END;
  } else {
    ObDocIdExt docid;
    if (OB_FAIL(result_docids_.prepare_allocate(heap_size))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < heap_size; i++) {
      const TopKItem &top_k_item = top_k_heap_.top();
      docid = id_cache_.at(top_k_item.cache_idx_);
      result_docids_[heap_size - 1 - i].from_datum(docid.get_datum());
      if (OB_FAIL(top_k_heap_.pop())) {
        LOG_WARN("failed to pop docid", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    result_docids_curr_iter_ = 0;
  }
  return ret;
}

int ObSPIVBMWIter::project_rows_from_result_docids(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (iter_param_->limit_param_->limit_ + iter_param_->limit_param_->offset_ == 0) {
    ret = OB_ITER_END;
  } else if (OB_INVALID_INDEX_INT64 == result_docids_curr_iter_) {
    if (OB_FAIL(reverse_top_k_heap())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to reverse top k heap", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_INDEX_INT64 == result_docids_curr_iter_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get heap iter", K(ret));
  } else if (result_docids_curr_iter_ == result_docids_.count()) {
    ret = OB_ITER_END;
  } else {
    count = OB_MIN(result_docids_.count() - result_docids_curr_iter_, capacity);
    ObExpr *id_proj_expr = iter_param_->id_proj_expr_;
    ObEvalCtx *eval_ctx = iter_param_->eval_ctx_;
    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
    sql::ObBitVector &id_evaluated_flags = id_proj_expr->get_evaluated_flags(*eval_ctx);
    for (int64_t i = 0; i < count; ++i) {
      guard.set_batch_idx(i);
      ObDatum &id_proj_datum = id_proj_expr->locate_datum_for_write(*eval_ctx);
      set_datum_func_(id_proj_datum, result_docids_.at(result_docids_curr_iter_++));
      id_evaluated_flags.set(i);
      id_proj_expr->set_evaluated_projected(*eval_ctx);
    }
  }
  return ret;
}

int ObSPIVBMWIter::process_collected_row(const ObDatum &id_datum, const double relevance)
{
  int ret = OB_SUCCESS;
  bool valid = false;
  if (is_pre_filter_) {
    ObDocIdExt docid;
    if (OB_FAIL(docid.from_datum(id_datum))) {
      LOG_WARN("failed to from datum", K(ret));
    } else {
      int hash_ret = valid_docid_set_.exist_refactored(docid);
      if (OB_HASH_EXIST == hash_ret) {
        valid = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (valid || !is_pre_filter_) {
    if (OB_UNLIKELY(top_k_heap_.count() > top_k_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected top k heap count", K(ret), K(top_k_heap_.count()), K_(top_k_count));
    } else if (top_k_heap_.count() < top_k_count_) {
      if (OB_FAIL(id_cache_.at(top_k_heap_.count()).from_datum(id_datum))) {
        LOG_WARN("failed to from datum", K(ret));
      } else if (OB_FAIL(top_k_heap_.push(TopKItem(relevance, top_k_heap_.count())))) {
        LOG_WARN("failed to push top k item", K(ret));
      }
    } else {
      const int64_t cache_idx = top_k_heap_.top().cache_idx_;
      const double top_k_threshold = get_top_k_threshold();
      if (relevance <= top_k_threshold) {
        // not a new top k candidate
      } else if (OB_FAIL(id_cache_.at(cache_idx).from_datum(id_datum))) {
        LOG_WARN("failed to from datum", K(ret));
      } else if (OB_FAIL(top_k_heap_.pop())) {
        LOG_WARN("failed to pop top k heap", K(ret));
      } else if (OB_FAIL(top_k_heap_.push(TopKItem(relevance, cache_idx)))) {
        LOG_WARN("failed to push top k item", K(ret));
      }
    }
  }

  return ret;
}

int ObSPIVBMWIter::init_before_wand_process()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
    ObSPIVBlockMaxDimIter *block_max_iter = static_cast<ObSPIVBlockMaxDimIter *>(dim_iters_->at(i));
    if (OB_FAIL(block_max_iter->init_block_max_iter())) {
      LOG_WARN("failed to init block max iter", K(ret));
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
