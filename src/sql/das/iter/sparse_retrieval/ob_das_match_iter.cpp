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

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_match_iter.h"
#include "sql/das/ob_das_ir_define.h"

namespace oceanbase
{
using namespace share;
namespace sql
{

ObDASMatchIter::ObDASMatchIter()
  : ObDASIter(ObDASIterType::DAS_ITER_ES_MATCH),
    mem_context_(nullptr),
    myself_allocator_(lib::ObMemAttr(MTL_ID(), "MatchIterSelf"), OB_MALLOC_NORMAL_BLOCK_SIZE),
    ir_match_part_score_ctdef_(nullptr),
    ir_match_part_score_rtdef_(nullptr),
    eval_ctx_(nullptr),
    domain_id_expr_(nullptr),
    set_datum_func_(nullptr),
    relevance_collector_(nullptr),
    children_relevance_exprs_(),
    children_domain_id_exprs_(),
    iter_domain_ids_(),
    next_round_iter_idxes_(),
    buffered_domain_ids_(),
    buffered_relevances_(),
    merge_cmp_(),
    merge_heap_(nullptr),
    next_round_cnt_(0),
    max_query_score_(-1.0),
    is_inited_(false)
{
}

ObDASMatchIter::~ObDASMatchIter()
{
}

int ObDASMatchIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  ObDASMatchIterParam &match_param = static_cast<ObDASMatchIterParam &>(param);
  if (OB_ISNULL(match_param.eval_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(match_param.eval_ctx_));
  } else if (match_param.children_relevance_exprs_.count() == 0) {
    // do nothing
  } else {
    lib::ContextParam mem_param;
    mem_param.set_mem_attr(MTL_ID(), "DasMatchIter", ObCtxIds::DEFAULT_CTX_ID);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, mem_param))) {
      LOG_WARN("failed to create das match iterator memory context", K(ret));
    }
    ir_match_part_score_ctdef_ = match_param.ir_match_part_score_ctdef_;
    ir_match_part_score_rtdef_ = match_param.ir_match_part_score_rtdef_;
    eval_ctx_ = match_param.eval_ctx_;
    domain_id_expr_ = match_param.domain_id_expr_;
    if (OB_ISNULL(domain_id_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null domain id expr", K(ret));
    } else if (domain_id_expr_->datum_meta_.type_ == common::ObUInt64Type && FALSE_IT(set_datum_func_ = ObISparseRetrievalMergeIter::set_datum_int)) {
    } else if (domain_id_expr_->datum_meta_.type_ != common::ObUInt64Type && FALSE_IT(set_datum_func_ = ObISparseRetrievalMergeIter::set_datum_shallow)) {
    } else if (OB_NOT_NULL(ir_match_part_score_rtdef_)) {
      const double query_boost = ir_match_part_score_rtdef_->match_boost_;
      if (query_boost <= 0.0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported query boost", K(ret), K(query_boost));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "query boost < 0 is");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(ir_match_part_score_ctdef_) && ir_match_part_score_rtdef_->match_fields_type_ == ObMatchFiledsType::MATCH_BEST_FIELDS) {
      ObDasBestfieldCollector *bestfield_collector = nullptr;
      if (OB_ISNULL(bestfield_collector = OB_NEWx(ObDasBestfieldCollector, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for inner product relevance collector", K(ret));
      } else if (OB_FAIL(bestfield_collector->init())) {
        LOG_WARN("failed to init boolean relevance collector", K(ret));
      } else {
        relevance_collector_ = bestfield_collector;
      }
    } else {
      ObSRDaaTInnerProductRelevanceCollector *inner_product_relevance_collector = nullptr;
      if (OB_ISNULL(inner_product_relevance_collector = OB_NEWx(ObSRDaaTInnerProductRelevanceCollector, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for inner product relevance collector", K(ret));
      } else if (OB_FAIL(inner_product_relevance_collector->init(0))) {
        LOG_WARN("failed to init boolean relevance collector", K(ret));
      } else {
        relevance_collector_ = inner_product_relevance_collector;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(children_relevance_exprs_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(children_relevance_exprs_.init(match_param.children_relevance_exprs_.count()))) {
      LOG_WARN("failed to init next round iter idxes", K(ret));
    } else if (OB_FAIL(children_relevance_exprs_.prepare_allocate(match_param.children_relevance_exprs_.count()))) {
      LOG_WARN("failed to prepare allocate next round iter idxes", K(ret));
    } else if (FALSE_IT(children_domain_id_exprs_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(children_domain_id_exprs_.init(match_param.children_domain_id_exprs_.count()))) {
      LOG_WARN("failed to init children domain id exprs", K(ret));
    } else if (OB_FAIL(children_domain_id_exprs_.prepare_allocate(match_param.children_domain_id_exprs_.count()))) {
      LOG_WARN("failed to prepare allocate children domain id exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < match_param.children_relevance_exprs_.count(); ++i) {
      children_relevance_exprs_[i] = match_param.children_relevance_exprs_.at(i);
      children_domain_id_exprs_[i] = match_param.children_domain_id_exprs_.at(i);
    }
    const int64_t children_cnt = children_relevance_exprs_.count();
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(next_round_iter_idxes_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(next_round_iter_idxes_.init(children_cnt))) {
      LOG_WARN("failed to init next round iter idxes", K(ret));
    } else if (OB_FAIL(next_round_iter_idxes_.prepare_allocate(children_cnt))) {
      LOG_WARN("failed to prepare allocate next round iter idxes", K(ret));
    } else if (FALSE_IT(iter_domain_ids_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(iter_domain_ids_.init(children_cnt))) {
      LOG_WARN("failed to init iter domain idxes", K(ret));
    } else if (OB_FAIL(iter_domain_ids_.prepare_allocate(children_cnt))) {
      LOG_WARN("failed to prepare allocate iter domain idxes", K(ret));
    } else if (FALSE_IT(buffered_domain_ids_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(buffered_domain_ids_.init(OB_MAX(1, eval_ctx_->max_batch_size_)))) {
      LOG_WARN("failed to init buffered domain idxes", K(ret));
    } else if (OB_FAIL(buffered_domain_ids_.prepare_allocate(OB_MAX(1, eval_ctx_->max_batch_size_)))) {
      LOG_WARN("failed to prepare allocate buffered domain idxes", K(ret));
    } else if (FALSE_IT(buffered_relevances_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(buffered_relevances_.init(OB_MAX(1, eval_ctx_->max_batch_size_)))) {
      LOG_WARN("failed to init buffered relevances", K(ret));
    } else if (OB_FAIL(buffered_relevances_.prepare_allocate(OB_MAX(1, eval_ctx_->max_batch_size_)))) {
      LOG_WARN("failed to prepare allocate buffered relevances", K(ret));
    } else if (OB_FAIL(merge_cmp_.init(domain_id_expr_->datum_meta_, &iter_domain_ids_))) {
      LOG_WARN("failed to init loser tree comparator", K(ret));
    } else if (children_cnt > 1 && OB_ISNULL(merge_heap_ = OB_NEWx(ObDASMatchMergeLoserTree, &myself_allocator_, merge_cmp_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate loser tree", K(ret));
    } else if (children_cnt > 1 && OB_FAIL(merge_heap_->init(children_cnt, children_cnt, myself_allocator_))) {
      LOG_WARN("failed to init iter loser tree", K(ret));
    } else if (children_cnt > 1 && OB_FAIL(merge_heap_->open(children_cnt))) {
      LOG_WARN("failed to open iter loser tree", K(ret));
    } else {
      next_round_cnt_ = children_cnt;
    }
  }

  if (OB_SUCC(ret)) {
    // ObExpr *output_relevance_expr = ir_match_part_score_ctdef_ ? ir_match_part_score_ctdef_->relevance_proj_col_;
    // if (OB_ISNULL(output_relevance_expr)) {
    //   ret = OB_ERR_UNEXPECTED;
    //   LOG_WARN("unexpected null output relevance expr", K(ret));
    // } else {
    //   output_relevance_expr->locate_datums_for_update(*eval_ctx_, OB_MAX(1, eval_ctx_->max_batch_size_));
    // }
    is_inited_ = true;
  }
  return ret;
}

int ObDASMatchIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (children_relevance_exprs_.count() != children_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected children relevance exprs count", K(ret), K(children_relevance_exprs_.count()), K(children_cnt_));
  } else {
    for (int64_t i = 0; i < next_round_iter_idxes_.count(); ++i) {
      next_round_iter_idxes_[i] = i;
    }
    next_round_cnt_ = children_cnt_;
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      ObDASIter *child = children_[i];
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null child", K(ret), K(i));
      } else if (OB_FAIL(child->do_table_scan())) {
        LOG_WARN("failed to do table scan", K(ret));
      }
    }
  }
  return ret;
}

int ObDASMatchIter::rescan()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    ObDASIter *child = children_[i];
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null child", K(ret), K(i));
    } else if (OB_FAIL(child->rescan())) {
      LOG_WARN("failed to do table scan", K(ret));
    }
  }
  return ret;
}

int ObDASMatchIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(merge_heap_)) {
    merge_heap_->reuse();
    merge_heap_->open(children_cnt_);
  }
  if (OB_NOT_NULL(relevance_collector_)) {
    relevance_collector_->reuse();
  }
  for (int64_t i = 0; i < next_round_iter_idxes_.count(); ++i) {
    next_round_iter_idxes_[i] = i;
  }
  next_round_cnt_ = children_cnt_;
  if (OB_NOT_NULL(mem_context_)) {
    mem_context_->reset_remain_one_page();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_FAIL(children_[i]->reuse())) {
      LOG_WARN("failed to reuse child", K(ret));
    }
  }
  return ret;
}

int ObDASMatchIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(merge_heap_)) {
    merge_heap_->~ObDASMatchMergeLoserTree();
    merge_heap_ = nullptr;
  }
  if (OB_NOT_NULL(relevance_collector_)) {
    relevance_collector_->reset();
    relevance_collector_ = nullptr;
  }
  children_relevance_exprs_.reset();
  children_domain_id_exprs_.reset();
  iter_domain_ids_.reset();
  next_round_iter_idxes_.reset();
  buffered_domain_ids_.reset();
  buffered_relevances_.reset();
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  myself_allocator_.reset();
  ir_match_part_score_ctdef_ = nullptr;
  ir_match_part_score_rtdef_ = nullptr;
  eval_ctx_ = nullptr;
  domain_id_expr_ = nullptr;
  set_datum_func_ = nullptr;
  next_round_cnt_ = 0;
  is_inited_ = false;
  return ret;
}

int ObDASMatchIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(children_cnt_ == 0)) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(merge_heap_) && children_cnt_ == 1) {
    if (OB_FAIL(children_[0]->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next rows failed", K(ret));
      }
    } else if (is_match_part_score_iter()) {
      ObExpr *input_relevance_expr = children_relevance_exprs_.at(0);
      ObExpr *output_relevance_expr = ir_match_part_score_ctdef_->relevance_proj_col_;
      const bool is_min_max_norm = ir_match_part_score_rtdef_->score_norm_function_ == ObMatchScoreNorm::SCORE_NORM_MIN_MAX;
      double max_query_score = 0.0;
      ObDASTRMergeIter *tr_merge_iter = is_min_max_norm ? static_cast<ObDASTRMergeIter *>(children_[0]) : nullptr;
      if (OB_ISNULL(input_relevance_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null domain id or input relevance expr", K(ret));
      } else if (input_relevance_expr != output_relevance_expr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain id expr", K(ret), K(input_relevance_expr), K(output_relevance_expr), K(ir_match_part_score_ctdef_->inv_scan_domain_id_col_));
      } else if (OB_NOT_NULL(tr_merge_iter) && OB_FAIL(tr_merge_iter->get_query_max_score(max_query_score))) {
        LOG_WARN("failed to get query max score", K(ret));
      } else if (OB_NOT_NULL(tr_merge_iter) && max_query_score <= 0.0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected query max score", K(ret), K(max_query_score));
      } else {
        const double query_boost = ir_match_part_score_rtdef_->match_boost_;
        const ObDatumVector &input_relevance_vector = input_relevance_expr->locate_expr_datumvector(*eval_ctx_);
        ObDatum *output_relevance_datums = output_relevance_expr->locate_batch_datums(*eval_ctx_);
        double result = input_relevance_vector.at(0)->get_double() * query_boost;
        if (is_min_max_norm && result > max_query_score) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          result = is_min_max_norm ? result / max_query_score : result;
          output_relevance_datums[0].set_double(result);
        }
      }
    } else if (is_match_score_iter()) {
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(children_relevance_exprs_.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null score expr", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected match iter type", K(ret));
    }
  } else if (OB_UNLIKELY(nullptr != merge_heap_)) {
    int64_t count = 0;
    if (OB_FAIL(do_one_merge_round(count))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to do one merge round", K(ret), K(count));
      }
    } else if (OB_FAIL(project_results(count))) {
      LOG_WARN("failed to project results", K(ret), K(count));
    }
  }
  return ret;
}



int ObDASMatchIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(children_cnt_ == 0)) {
    count = 0;
    ret = OB_ITER_END;
  } else if (OB_ISNULL(merge_heap_) && children_cnt_ == 1) {
    if (OB_FAIL(children_[0]->get_next_rows(count, capacity))) {
      if (OB_ITER_END == ret) {
        ret = count == 0 ? OB_ITER_END : OB_SUCCESS;
      } else {
        LOG_WARN("get next rows failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_match_part_score_iter()) {
      ObExpr *input_relevance_expr = children_relevance_exprs_.at(0);
      ObExpr *output_relevance_expr = ir_match_part_score_ctdef_->relevance_proj_col_;
      const bool is_min_max_norm = ir_match_part_score_rtdef_->score_norm_function_ == ObMatchScoreNorm::SCORE_NORM_MIN_MAX;
      double max_query_score = 0.0;
      ObDASTRMergeIter *tr_merge_iter = is_min_max_norm ? static_cast<ObDASTRMergeIter *>(children_[0]) : nullptr;
      if (OB_ISNULL(input_relevance_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null domain id or input relevance expr", K(ret));
      } else if (input_relevance_expr != output_relevance_expr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected domain id expr", K(ret), K(input_relevance_expr), K(output_relevance_expr), K(ir_match_part_score_ctdef_->inv_scan_domain_id_col_));
      } else if (OB_NOT_NULL(tr_merge_iter) && OB_FAIL(tr_merge_iter->get_query_max_score(max_query_score))) {
        LOG_WARN("failed to get query max score", K(ret));
      } else if (OB_NOT_NULL(tr_merge_iter) && max_query_score <= 0.0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected query max score", K(ret), K(max_query_score));
      } else {
        const double query_boost = ir_match_part_score_rtdef_->match_boost_;
        const ObDatumVector &input_relevance_vector = input_relevance_expr->locate_expr_datumvector(*eval_ctx_);
        ObDatum *output_relevance_datums = output_relevance_expr->locate_batch_datums(*eval_ctx_);
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          double result = input_relevance_vector.at(i)->get_double() * query_boost;
          if (is_min_max_norm && result > max_query_score * query_boost) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected result", K(ret), K(result), K(max_query_score), K(query_boost), K(input_relevance_vector.at(i)->get_double()));
          } else {
            result = is_min_max_norm ? result / max_query_score : result;
            output_relevance_datums[i].set_double(result);
          }
        }
      }
    } else if (is_match_score_iter()) {
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(children_relevance_exprs_.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null score expr", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected match iter type", K(ret));
    }
  } else if (OB_UNLIKELY(nullptr != merge_heap_)) {
    count = 0;
    const int64_t real_capacity = MIN(capacity, eval_ctx_->max_batch_size_);
    while (OB_SUCC(ret) && count < real_capacity) {
      if (OB_FAIL(do_one_merge_round(count))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to do one merge round", K(ret), K(count));
        }
      }
    }
    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      if (count > 0 && OB_FAIL(project_results(count))) {
        LOG_WARN("failed to project results", K(ret), K(count));
      }
    }
  }
  return ret;
}

int ObDASMatchIter::do_one_merge_round(int64_t &count)
{
  int ret = OB_SUCCESS;
  bool need_project = true;
  double relevance = 0.0;
  const ObDatum *id_datum = nullptr;
  if (OB_FAIL(fill_merge_heap())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to fill merge heap", K(ret));
    }
  } else if (OB_FAIL(collect_dims_by_id(id_datum, relevance, need_project))) {
    LOG_WARN("failed to merge dimensions", K(ret));
  } else if (OB_FAIL(need_project && filter_on_demand(count, relevance, need_project))) {
    LOG_WARN("failed to process filter", K(ret));
  } else if (OB_FAIL(need_project && cache_result(count, *id_datum, relevance))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to cache result", K(ret));
    }
  }
  return ret;
}

int ObDASMatchIter::fill_merge_heap()
{
  int ret = OB_SUCCESS;
  ObSRMergeItem item;
  const bool is_min_max_norm = is_match_part_score_iter() && ir_match_part_score_rtdef_->score_norm_function_ == ObMatchScoreNorm::SCORE_NORM_MIN_MAX;
  const bool need_get_max_query_score = is_match_part_score_iter() && max_query_score_ < 0.0 && is_min_max_norm;
  if (need_get_max_query_score) {
    max_query_score_ = 0.0;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < next_round_cnt_; ++i) {
    const int64_t iter_idx = next_round_iter_idxes_[i];
    ObDASIter *dim_iter = nullptr;
    if (OB_ISNULL(dim_iter = children_[iter_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null dimension iter", K(ret), K(iter_idx));
    } else if (OB_FAIL(dim_iter->get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to try load next batch dimension data", K(ret));\
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      ObDatum &domain_id = children_domain_id_exprs_.at(iter_idx)->locate_expr_datum(*eval_ctx_, 0);
      ObDatum &input_relevance = children_relevance_exprs_.at(iter_idx)->locate_expr_datum(*eval_ctx_, 0);
      item.relevance_ = input_relevance.get_double();
      iter_domain_ids_[iter_idx].from_datum(domain_id);
      item.iter_idx_ = iter_idx;
      if (is_match_part_score_iter()) {
        if (need_get_max_query_score) {
          double max_query_score = 0.0;
          ObDASTRMergeIter *tr_merge_iter = static_cast<ObDASTRMergeIter *>(children_[iter_idx]);
          if (OB_ISNULL(tr_merge_iter)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null tr merge iter", K(ret));
          } else if (OB_FAIL(tr_merge_iter->get_query_max_score(max_query_score))) {
            LOG_WARN("failed to get query max score", K(ret));
          } else if (max_query_score <= 0.0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected query max score", K(ret), K(max_query_score));
          } else {
            max_query_score_ += max_query_score;
          }
        }
        if (OB_FAIL(ret)) {
        } else {
          const double query_boost = ir_match_part_score_rtdef_->match_boost_;
          item.relevance_ = item.relevance_ * query_boost;
        }
      } else if (is_match_score_iter()) {
        // donothing
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected match iter type", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(merge_heap_->push(item))) {
        LOG_WARN("fail to push item to merge heap", K(ret), K(item));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (merge_heap_->empty()) {
    ret = OB_ITER_END;
  } else if (0 != next_round_cnt_ && OB_FAIL(merge_heap_->rebuild())) {
    LOG_WARN("fail to rebuild merge heap", K(ret));
  } else {
    next_round_cnt_ = 0;
  }
  return ret;
}

int ObDASMatchIter::collect_dims_by_id(const ObDatum *&id_datum, double &relevance, bool &got_valid_id)
{
  int ret = OB_SUCCESS;

  const ObSRMergeItem *top_item = nullptr;
  bool curr_doc_end = false;
  int64_t iter_idx = 0;
  relevance = 0.0;
  got_valid_id = false;
  relevance_collector_->reuse();
  while (OB_SUCC(ret) && !merge_heap_->empty() && !curr_doc_end) {
    if (merge_heap_->is_unique_champion()) {
      curr_doc_end = true;
    }
    if (OB_FAIL(merge_heap_->top(top_item))) {
      LOG_WARN("failed to get top item from merge heap", K(ret));
    } else if (OB_FAIL(relevance_collector_->collect_one_dim(top_item->iter_idx_, top_item->relevance_))) {
      LOG_WARN("failed to collect one dimension", K(ret));
    } else if (FALSE_IT(iter_idx = top_item->iter_idx_)) {
    } else if (OB_FAIL(merge_heap_->pop())) {
      LOG_WARN("failed to pop top item in heap", K(ret));
    } else {
      next_round_iter_idxes_[next_round_cnt_++] = iter_idx;
    }
  }

  if (OB_SUCC(ret)) {
    id_datum = &iter_domain_ids_[iter_idx].get_datum();
    if (OB_ISNULL(id_datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null id datum", K(ret));
    } else if (OB_FAIL(relevance_collector_->get_result(relevance, got_valid_id))) {
      LOG_WARN("failed to get result", K(ret));
    } else if (got_valid_id &&OB_FAIL(process_collected_row(*id_datum, relevance))) {
      LOG_WARN("failed to process collected row", K(ret));
    } else if (is_match_part_score_iter() && ir_match_part_score_rtdef_->score_norm_function_ == ObMatchScoreNorm::SCORE_NORM_MIN_MAX) {
      relevance = relevance / max_query_score_;
    }
  }

  return ret;
}

int ObDASMatchIter::filter_on_demand(const int64_t count, const double relevance, bool &need_project)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObDASMatchIter::cache_result(int64_t &count, const ObDatum &id_datum, const double relevance)
{
  int ret = OB_SUCCESS;
  buffered_domain_ids_[count].from_datum(id_datum);
  buffered_relevances_[count] = relevance;
  ++count;
  return ret;
}

int ObDASMatchIter::project_results(const int64_t count)
{
  int ret = OB_SUCCESS;
  ObExpr *relevance_proj_expr = is_match_score_iter()? children_relevance_exprs_.at(0) : ir_match_part_score_ctdef_->relevance_proj_col_;
  ObExpr *id_proj_expr = domain_id_expr_;
  ObEvalCtx *eval_ctx = eval_ctx_;
  ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
  bool need_project_relevance = true;
  if (eval_ctx->is_vectorized()) {
    sql::ObBitVector &id_evaluated_flags = id_proj_expr->get_evaluated_flags(*eval_ctx);
    for (int64_t i = 0; i < count; ++i) {
      guard.set_batch_idx(i);
      ObDatum &id_proj_datum = id_proj_expr->locate_datum_for_write(*eval_ctx);
      set_datum_func_(id_proj_datum, buffered_domain_ids_[i]);
      id_evaluated_flags.set(i);
      id_proj_expr->set_evaluated_projected(*eval_ctx);
    }
    if (need_project_relevance) {
      sql::ObBitVector &relevance_evaluated_flags = relevance_proj_expr->get_evaluated_flags(*eval_ctx);
      for (int64_t i = 0; i < count; ++i) {
        guard.set_batch_idx(i);
        ObDatum &relevance_proj_datum = relevance_proj_expr->locate_datum_for_write(*eval_ctx);
        relevance_proj_datum.set_double(buffered_relevances_[i]);
        relevance_evaluated_flags.set(i);
        relevance_proj_expr->set_evaluated_projected(*eval_ctx);
        for (int64_t j = 1; is_match_score_iter() && j < children_relevance_exprs_.count(); ++j) {
          // TODO: trick. just use one expr to project, the other exprs is filled with 0.
          ObDatum &tmp_relevance_proj_datum = children_relevance_exprs_.at(j)->locate_datum_for_write(*eval_ctx);
          tmp_relevance_proj_datum.set_double(0);
          children_relevance_exprs_.at(j)->get_evaluated_flags(*eval_ctx).set(i);
          children_relevance_exprs_.at(j)->set_evaluated_projected(*eval_ctx);
        }
      }
    }
  } else if (OB_UNLIKELY(1 != count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected number of results to project", K(ret), K(count));
  } else {
    guard.set_batch_idx(0);
    ObDatum &id_proj_datum = id_proj_expr->locate_datum_for_write(*eval_ctx);
    set_datum_func_(id_proj_datum, buffered_domain_ids_[0]);
    id_proj_expr->set_evaluated_projected(*eval_ctx);
    ObDatum &relevance_proj_datum = relevance_proj_expr->locate_datum_for_write(*eval_ctx);
    relevance_proj_datum.set_double(buffered_relevances_[0]);
    relevance_proj_expr->set_evaluated_projected(*eval_ctx);
    for (int64_t j = 1; is_match_score_iter() && j < children_relevance_exprs_.count(); ++j) {
      ObDatum &tmp_relevance_proj_datum = children_relevance_exprs_.at(j)->locate_datum_for_write(*eval_ctx);
      tmp_relevance_proj_datum.set_double(0);
      children_relevance_exprs_.at(j)->set_evaluated_projected(*eval_ctx);
    }
  }


  return ret;
}

int ObDasBestfieldCollector::init()
{
  int ret = OB_SUCCESS;
  max_relevance_ = 0.0;
  return ret;
}

void ObDasBestfieldCollector::reset()
{
  max_relevance_ = 0.0;
}

void ObDasBestfieldCollector::reuse()
{
  max_relevance_ = 0.0;
}

int ObDasBestfieldCollector::collect_one_dim(const int64_t dim_idx, const double relevance)
{
  int ret = OB_SUCCESS;
  max_relevance_ = std::max(max_relevance_, relevance);
  return ret;
}

int ObDasBestfieldCollector::get_result(double &relevance, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = max_relevance_ > 0.0;
  relevance = max_relevance_;
  return ret;
}

ObDASMatchMergeCmp::ObDASMatchMergeCmp()
  : cmp_func_(nullptr),
    iter_ids_(nullptr),
    is_inited_(false)
{
}

int ObDASMatchMergeCmp::init(ObDatumMeta id_meta, const ObFixedArray<ObDocIdExt, ObIAllocator> *iter_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_ids)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(iter_ids));
  } else {
    iter_ids_ = iter_ids;
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(id_meta.type_, id_meta.cs_type_);
    cmp_func_ = lib::is_oracle_mode() ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
    if (OB_ISNULL(cmp_func_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to init IRIterLoserTreeCmp", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDASMatchMergeCmp::cmp(
    const ObSRMergeItem &l,
    const ObSRMergeItem &r,
    int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    int tmp_ret = 0;
    if (OB_FAIL(cmp_func_(get_id_datum(l.iter_idx_), get_id_datum(r.iter_idx_), tmp_ret))) {
      LOG_WARN("failed to compare doc id by datum", K(ret));
    } else {
      cmp_ret = tmp_ret;
    }
  }
  return ret;
}

int ObDASMatchIter::get_match_param(const ObDASIREsMatchCtDef *match_ctdef,
                           ObDASIREsMatchRtDef *match_rtdef,
                           common::ObIAllocator &alloc,
                           int &minimum_should_match)
{
  int ret = OB_SUCCESS;
  ObExpr *param_text = nullptr;
  ObEvalCtx *eval_ctx = nullptr;
  ObDatum *param_text_datum = nullptr;
  if (OB_ISNULL(match_rtdef) || OB_ISNULL(match_ctdef) || OB_ISNULL(match_ctdef->es_param_text_expr_) || OB_ISNULL(match_rtdef->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret),KP(eval_ctx));
  } else if (FALSE_IT(param_text = match_ctdef->es_param_text_expr_)) {
  } else if (FALSE_IT(eval_ctx = match_rtdef->eval_ctx_)) {
  } else if (OB_FAIL(param_text->eval(*eval_ctx, param_text_datum))) {
    LOG_WARN("expr evaluation failed", K(ret));
  } else if (0 == param_text_datum->len_) {
    minimum_should_match = 0;
    match_rtdef->match_boost_ = 1.0;
    match_rtdef->match_operator_ = ObMatchOperator::MATCH_OPERATOR_OR;
    match_rtdef->score_norm_function_ = ObMatchScoreNorm::SCORE_NORM_NONE;
    match_rtdef->match_fields_type_ = ObMatchFiledsType::MATCH_BEST_FIELDS;
  } else {
    const ObString &param_string = param_text_datum->get_string();
    const ObCollationType &cs_type = param_text->datum_meta_.cs_type_;
    const ObCollationType dst_type = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;
    ObString str_dest;
    if (cs_type != dst_type) {
      ObString tmp_out;
      if (OB_FAIL(ObCharset::tolower(cs_type, param_string, tmp_out, alloc))) {
        LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(param_string));
      } else if (OB_FAIL(common::ObCharset::charset_convert(alloc, tmp_out, cs_type, dst_type, str_dest))) {
        LOG_WARN("failed to convert string", K(ret), K(cs_type), K(param_string));
      }
    } else if (OB_FAIL(ObCharset::tolower(cs_type, param_string, str_dest, alloc))){
      LOG_WARN("failed to casedown string", K(ret), K(cs_type), K(param_string));
    }
    if (OB_SUCC(ret)) {
      minimum_should_match = 0;
      match_rtdef->match_boost_ = 1.0;
      match_rtdef->match_operator_ = ObMatchOperator::MATCH_OPERATOR_OR;
      match_rtdef->score_norm_function_ = ObMatchScoreNorm::SCORE_NORM_NONE;
      match_rtdef->match_fields_type_ = ObMatchFiledsType::MATCH_BEST_FIELDS;
      // TODO: change charset to utf8mb4_general_ci
      char split_tag = ';';
      char equl_tag = '=';
      ObString param_operator("operator");
      ObString param_boost("boost");
      ObString param_minimum_should_match("minimum_should_match");
      ObString param_score_norm("score_norm");
      ObString param_type("type");
      double boost_value = 1.0;
      ObConstRawExpr *boost_expr = nullptr;
      int should_match_value = 1;
      ObConstRawExpr *should_match_expr = nullptr;
      while (!str_dest.empty()) {
        ObString param_str = str_dest.split_on(split_tag);
        if (param_str.empty()) {
          param_str = str_dest;
          str_dest.reset();
        }
        ObString child_param_key = param_str.split_on(equl_tag).trim();
        if (child_param_key.empty()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "this match param");
          LOG_WARN("unexpected operator", K(ret), K(param_str));
        } else if (child_param_key.compare_equal(param_operator)) {
          if (param_str.compare_equal("and")) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "match operator 'and' ");
            LOG_WARN("match operator and is not supported", K(ret), K(param_str));
          } else if (param_str.compare_equal("or")) {
            match_rtdef->match_operator_ = ObMatchOperator::MATCH_OPERATOR_OR;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this match operator");
            LOG_WARN("unexpected operator", K(ret), K(param_str));
          }
        } else if (child_param_key.compare_equal(param_boost)) {
          char *boost_str = static_cast<char *>(alloc.alloc(param_str.length() + 1));
          if (OB_ISNULL(boost_str)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else {
            memcpy(boost_str, param_str.ptr(), param_str.length());
            boost_str[param_str.length()] = '\0';
            boost_value= atof(boost_str);
          }
          if (OB_SUCC(ret) && boost_value <= 0) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "positive boost value");
            LOG_WARN("boost value is not positive", K(ret), K(boost_value));
          } else {
            match_rtdef->match_boost_ = boost_value;
          }
        } else if (child_param_key.compare_equal(param_minimum_should_match)) {
          char *should_match_str = static_cast<char *>(alloc.alloc(param_str.length() + 1));
          if (OB_ISNULL(should_match_str)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else {
            memcpy(should_match_str, param_str.ptr(), param_str.length());
            should_match_str[param_str.length()] = '\0';
            should_match_value = atoi(should_match_str);
          }
          if (OB_SUCC(ret) && should_match_value <= 0) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "positive minimum_should_match value");
            LOG_WARN("minimum_should_match value is not positive", K(ret), K(should_match_value));
          } else {
            minimum_should_match = should_match_value;
          }
        } else if (child_param_key.compare_equal(param_score_norm)) {
          if (param_str.compare_equal("min-max")) {
            match_rtdef->score_norm_function_ = ObMatchScoreNorm::SCORE_NORM_MIN_MAX;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this score_norm");
            LOG_WARN("unexpected operator", K(ret), K(param_str));
          }
        } else if (child_param_key.compare_equal(param_type)) {
          if (param_str.compare_equal("most_fields")) {
            match_rtdef->match_fields_type_ = ObMatchFiledsType::MATCH_MOST_FIELDS;
          } else if (param_str.compare_equal("best_fields")) {
            match_rtdef->match_fields_type_ = ObMatchFiledsType::MATCH_BEST_FIELDS;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this score_norm");
            LOG_WARN("unexpected operator", K(ret), K(param_str));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
