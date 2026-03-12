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

#include "ob_disjunctive_max_iter.h"

namespace oceanbase
{
namespace storage
{

ObSRDisjunctiveMaxIter::ObSRDisjunctiveMaxIter()
  : ObISparseRetrievalMergeIter(),
    iter_allocator_(nullptr),
    topk_iters_(nullptr),
    es_match_ctdef_(nullptr),
    es_match_rtdef_(nullptr),
    ir_scan_ctdefs_(nullptr),
    ir_scan_rtdefs_(nullptr),
    topk_limit_(0),
    norm_(0.0),
    score_cmp_(),
    heap_(score_cmp_, iter_allocator_),
    hash_map_(),
    set_datum_func_(nullptr)
{}

int ObSRDisjunctiveMaxIter::init(ObSRDisjunctiveMaxIterParam &iter_param,
                                 ObIArray<ObSRBlockMaxTopKIterImpl *> &topk_iters,
                                 ObIAllocator &iter_allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_ISNULL(iter_param.es_match_ctdef_) || OB_ISNULL(iter_param.es_match_rtdef_)
      || OB_ISNULL(iter_param.ir_scan_ctdefs_) || OB_ISNULL(iter_param.ir_scan_rtdefs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_UNLIKELY(iter_param.topk_limit_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected topk limit", K(ret), K_(iter_param.topk_limit));
  } else if (OB_UNLIKELY(topk_iters.empty())) {
    // skip the following
  } else {
    iter_allocator_ = &iter_allocator;
    topk_iters_.set_allocator(&iter_allocator);
    es_match_ctdef_ = iter_param.es_match_ctdef_;
    es_match_rtdef_ = iter_param.es_match_rtdef_;
    ir_scan_ctdefs_ = iter_param.ir_scan_ctdefs_;
    ir_scan_rtdefs_ = iter_param.ir_scan_rtdefs_;
    topk_limit_ = iter_param.topk_limit_;
    ObDatumMeta id_meta = es_match_ctdef_->inv_scan_domain_id_col_->datum_meta_;
    if (OB_FAIL(topk_iters_.assign(topk_iters))) {
      LOG_WARN("failed to assign topk iters", K(ret));
    } else if (OB_FAIL(hash_map_.create(topk_iters_.count() * topk_limit_,
                                        common::ObMemAttr(MTL_ID(), "FTTopKMap")))) {
      LOG_WARN("failed to create hash map", K(ret));
    } else if (common::ObUInt64Type == id_meta.type_) {
      set_datum_func_ = ObISparseRetrievalMergeIter::set_datum_int;
    } else {
      set_datum_func_ = ObISparseRetrievalMergeIter::set_datum_shallow;
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

void ObSRDisjunctiveMaxIter::reuse(const bool switch_tablet)
{
  for (int64_t i = 0; i < topk_iters_.count(); ++i) {
    topk_iters_.at(i)->reuse();
  }
  while (!heap_.empty()) {
    heap_.pop();
  }
  hash_map_.clear();
  id_cache_.reset();
}

void ObSRDisjunctiveMaxIter::reset()
{
  for (int64_t i = 0; i < topk_iters_.count(); ++i) {
    topk_iters_.at(i)->reset();
  }
  heap_.reset();
  hash_map_.destroy();
  id_cache_.reset();
  is_inited_ = false;
}

int ObSRDisjunctiveMaxIter::get_next_row()
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_FAIL(get_next_rows(1, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row", K(ret));
    } else if (OB_UNLIKELY(count != 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected row count", K(ret), K(count));
    }
  } else if (OB_UNLIKELY(count != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected row count", K(ret), K(count));
  }
  return ret;
}

int ObSRDisjunctiveMaxIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(0 == capacity)) {
    count = 0;
  } else if (OB_UNLIKELY(topk_iters_.empty())) {
    ret = OB_ITER_END;
  } else if (heap_.empty() && OB_FAIL(load_results())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to load results", K(ret), K(capacity), K(count));
    }
  } else if (OB_FAIL(project_results(capacity, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to project results", K(ret), K(capacity), K(count));
    }
  }
  return ret;
}

int ObSRDisjunctiveMaxIter::load_results()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!heap_.empty()) || OB_UNLIKELY(!hash_map_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-empty heap or hash map", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < topk_iters_.count(); ++i) {
    int64_t count = 0;
    int64_t sub_count = 0;
    while (OB_SUCC(ret) && count < topk_limit_) {
      sub_count = 0;
      if (OB_FAIL(topk_iters_.at(i)->get_next_rows(topk_limit_ - count, sub_count))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next rows from topk iter", K(ret));
        } else if (sub_count > 0) {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret)) {
        count += sub_count;
        if (OB_UNLIKELY(count > topk_limit_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected count", K(ret), K_(topk_limit), K(count), K(sub_count));
        }
      }
      LOG_DEBUG("disjunctive max iter loading results", K(ret),
                K(i), K_(topk_limit), K(count), K(sub_count));
      ObEvalCtx &eval_ctx = *es_match_rtdef_->eval_ctx_;
      const ObDatumVector &id_datums = ir_scan_ctdefs_->at(i)->inv_scan_domain_id_col_
          ->locate_expr_datumvector(eval_ctx);
      const ObDatumVector &relevance_datums = ir_scan_ctdefs_->at(i)->relevance_proj_col_
          ->locate_expr_datumvector(eval_ctx);
      ObDocIdExt cur_id;
      TopKItem prev_item;
      for (int64_t j = 0; OB_SUCC(ret) && j < sub_count; ++j) {
        const ObDatum *id_datum = id_datums.at(j);
        double relevance = relevance_datums.at(j)->get_double();
        if (OB_FAIL(cur_id.from_datum(*id_datum))) {
          LOG_WARN("failed to get cur id from datum", K(ret));
        } else if (OB_FAIL(hash_map_.get_refactored(cur_id, prev_item))) {
          if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
            LOG_WARN("failed to get prev item from hash map", K(ret));
          } else {
            const int64_t cache_idx = id_cache_.count();
            if (OB_FAIL(id_cache_.push_back(cur_id))) {
              LOG_WARN("failed to cache id", K(ret));
            } else if (OB_FAIL(hash_map_.set_refactored(cur_id,
                                                        TopKItem(relevance, cache_idx),
                                                        0/*overwrite*/))) {
              LOG_WARN("failed to set cur item in hash map", K(ret));
            }
          }
        } else if (relevance <= prev_item.relevance_) {
          // discard this result, continue
        } else if (OB_FAIL(hash_map_.set_refactored(cur_id,
                                                    TopKItem(relevance, prev_item.cache_idx_),
                                                    1/*overwrite*/))) {
          LOG_WARN("failed to set cur item in hash map", K(ret));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (SCORE_NORM_MIN_MAX != es_match_rtdef_->score_norm_function_) {
    norm_ = 1.0;
  } else {
    norm_ = 0.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < topk_iters_.count(); ++i) {
      double max_score = 0.0;
      if (OB_FAIL(topk_iters_.at(i)->get_query_max_score(max_score))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get query max score", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (max_score > norm_) {
        norm_ = max_score;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(0.0 == norm_)) {
      ret = OB_ITER_END;
    } else {
      norm_ /= es_match_rtdef_->match_boost_;
    }
  }
  for (TopKHashMap::const_iterator iter = hash_map_.begin();
      OB_SUCC(ret) && iter != hash_map_.end();
      ++iter) {
    double relevance = iter->second.relevance_;
    if (OB_UNLIKELY(relevance <= 0.0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected relevance", K(ret), K(relevance));
    } else if (heap_.count() < topk_limit_) {
      if (OB_FAIL(heap_.push(iter->second))) {
        LOG_WARN("failed to push into heap", K(ret));
      }
    } else if (heap_.top().relevance_ < relevance) {
      if (OB_FAIL(heap_.pop())) {
        LOG_WARN("failed to pop from heap", K(ret));
      } else if (OB_FAIL(heap_.push(iter->second))) {
        LOG_WARN("failed to push into heap", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(heap_.count() > topk_limit_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected heap size", K(ret), K(heap_.count()), K_(topk_limit));
  }
  return ret;
}

int ObSRDisjunctiveMaxIter::project_results(const int64_t capacity, int64_t &count) {
  int ret = OB_SUCCESS;
  count = 0;
  ObEvalCtx &eval_ctx = *es_match_rtdef_->eval_ctx_;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx);
  ObExpr *id_proj_expr = es_match_ctdef_->inv_scan_domain_id_col_;
  ObExpr *relevance_proj_expr = es_match_ctdef_->relevance_proj_col_;
  sql::ObBitVector &id_evaluated_flags = id_proj_expr->get_evaluated_flags(eval_ctx);
  sql::ObBitVector &relevance_evaluated_flags = relevance_proj_expr->get_evaluated_flags(eval_ctx);
  TopKItem latest_item;
  while (OB_SUCC(ret) && count < capacity && !heap_.empty()) {
    guard.set_batch_idx(count);
    ObDatum &id_proj_datum = id_proj_expr->locate_datum_for_write(eval_ctx);
    const ObDocIdExt &top_id = id_cache_.at(heap_.top().cache_idx_);
    set_datum_func_(id_proj_datum, top_id);
    id_evaluated_flags.set(count);
    ObDatum &relevance_proj_datum = relevance_proj_expr->locate_datum_for_write(eval_ctx);
    relevance_proj_datum.set_double(heap_.top().relevance_ / norm_);
    relevance_evaluated_flags.set(count);
    ++count;
    if (OB_FAIL(heap_.pop())) {
      LOG_WARN("failed to pop from heap", K(ret));
    }
  }
  if (OB_SUCC(ret) && heap_.empty()) {
    ret = OB_ITER_END;
  }
  id_proj_expr->set_evaluated_projected(eval_ctx);
  relevance_proj_expr->set_evaluated_projected(eval_ctx);
  return ret;
}

} // namespace storage
} // namespace oceanbase
