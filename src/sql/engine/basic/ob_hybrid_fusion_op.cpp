/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/utility/utility.h"
#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_hybrid_fusion_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

// Constants
static const double EPSILON = 1e-6;

ObHybridFusionSpec::ObHybridFusionSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
    fusion_method_(ObFusionMethod::WEIGHT_SUM),
    min_score_expr_(nullptr),
    rank_window_size_expr_(nullptr),
    rank_constant_expr_(nullptr),
    size_expr_(nullptr),
    offset_expr_(nullptr),
    weights_exprs_(alloc),
    score_exprs_(alloc),
    path_top_k_limit_exprs_(alloc),
    score_expr_output_indices_(alloc),
    search_index_(-1),
    fusion_iter_exec_mode_(ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS)
{
}

OB_SERIALIZE_MEMBER((ObHybridFusionSpec, ObOpSpec),
                     fusion_method_,
                     min_score_expr_,
                     rank_window_size_expr_,
                     rank_constant_expr_,
                     size_expr_,
                     offset_expr_,
                     weights_exprs_,
                     score_exprs_,
                     path_top_k_limit_exprs_,
                     score_expr_output_indices_,
                     search_index_,
                     fusion_iter_exec_mode_);

int ObHybridFusionOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    lib::ObMemAttr mem_attr(tenant_id, "HybridFusionRS", ObCtxIds::WORK_AREA);
    const int64_t max_batch_size = eval_ctx_.max_batch_size_ > 0 ? eval_ctx_.max_batch_size_ : 1; // default to 1 if 0
    const int64_t mem_limit = 0;
    if (OB_FAIL(row_store_.init(child_->get_spec().output_,
                                 max_batch_size,
                                 mem_attr,
                                 mem_limit,
                                 true,  // enable_dump
                                 0,     // row_extra_size
                                 common::ObCompressorType::NONE_COMPRESSOR))) {
      LOG_WARN("failed to init row store", K(ret));
    } else {
      const int64_t estimated_size = 0;
      if (OB_FAIL(sql_mem_processor_.init(&ctx_.get_allocator(),
                                           tenant_id,
                                           estimated_size,
                                           spec_.type_,
                                           spec_.id_,
                                           &ctx_))) {
        LOG_WARN("failed to init sql memory manager processor", K(ret));
      } else {
        row_store_.set_callback(&sql_mem_processor_);
        row_store_.set_io_event_observer(&io_event_observer_);
        row_store_.set_dir_id(sql_mem_processor_.get_dir_id());
        if (OB_FAIL(row_store_reader_.init(&row_store_))) {
          LOG_WARN("failed to init row store reader", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_constant_params())) {
        LOG_WARN("failed to init constant exprs", K(ret));
      } else if (OB_FAIL(init_path_heaps())) {
        LOG_WARN("failed to init path heaps", K(ret));
      } else if (OB_FAIL(batch_doc_indices_.prepare_allocate(max_batch_size))) {
        LOG_WARN("failed to allocate batch_doc_indices", K(ret));
      } else if (OB_FAIL(stored_rows_buffer_.prepare_allocate(max_batch_size))) {
        LOG_WARN("failed to allocate stored_rows_buffer", K(ret));
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::inner_rescan()
{
  int ret = OB_SUCCESS;

  is_data_ready_ = false;
  output_idx_ = 0;
  full_recall_phase_ = PHASE_STREAMING;
  row_store_reader_.reset();
  row_store_.reuse();
  if (row_store_.is_inited()) {
    row_store_reader_.init(&row_store_);
  }
  fusion_docs_.reset();
  path_stats_.reset();
  sorted_doc_indices_.reset();
  stored_rows_buffer_.reset();
  comparers_.reset();
  weights_.reset();
  path_top_k_limit_.reset();
  passthrough_doc_indices_.reset();
  top_k_doc_indices_.reuse();
  for (int64_t i = 0; i < path_heaps_.count(); ++i) {
    path_heaps_.at(i)->reset();
  }
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

int ObHybridFusionOp::inner_close()
{
  int ret = OB_SUCCESS;
  sql_mem_processor_.unregister_profile();
  is_data_ready_ = false;
  output_idx_ = 0;
  row_store_reader_.reset();
  fusion_docs_.reset();
  path_stats_.reset();
  sorted_doc_indices_.reset();
  stored_rows_buffer_.reset();
  passthrough_doc_indices_.reset();
  for (int64_t i = 0; i < path_heaps_.count(); ++i) {
    if (path_heaps_.at(i) != nullptr) {
      path_heaps_.at(i)->~ObTopKHeap();
    }
  }
  path_heaps_.reset();
  comparers_.reset();
  weights_.reset();
  path_top_k_limit_.reset();
  row_store_.reset();
  if (OB_FAIL(ObOperator::inner_close())) {
    LOG_WARN("failed to close operator", K(ret));
  }
  return ret;
}

int ObHybridFusionOp::inner_get_next_row()
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("inner_get_next_row is not implemented", K(ret));
  return ret;
}

int ObHybridFusionOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (fusion_iter_exec_mode_ != ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS) {
    if (search_index_ < 0) {
      if (OB_FAIL(get_next_batch_knn_only(max_row_cnt))) {
        LOG_WARN("failed to get next batch in knn-only path", K(ret), K(max_row_cnt));
      }
    } else if (OB_FAIL(get_next_batch_query_and_knn(max_row_cnt))) {
      LOG_WARN("failed to get next batch in query+knn path", K(ret), K(max_row_cnt));
    }
  } else if (OB_FAIL(get_next_batch_score_topk_query(max_row_cnt))) {
    LOG_WARN("failed to get next batch in topk path", K(ret), K(max_row_cnt));
  }

  if (OB_SUCC(ret) && is_data_ready_) {
    if (OB_FAIL(emit_output_batch(max_row_cnt))) {
      LOG_WARN("failed to emit output batch", K(ret), K(max_row_cnt));
    }
  }
  return ret;
}

int ObHybridFusionOp::get_next_batch_score_topk_query(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (top_k_limit_ == 0 || size_ == 0) {
    ret = OB_ITER_END;
  } else if (!is_data_ready_) {
    if (OB_FAIL(collect_all_data_batch())) {
      LOG_WARN("failed to collect all data batch", K(ret));
    } else if (OB_FAIL(get_top_k_doc_indices())) {
      LOG_WARN("failed to get top k doc indices", K(ret));
    } else if (OB_FAIL(rescore())) {
      LOG_WARN("failed to rescore", K(ret));
    } else if (OB_FAIL(compute_fusion_topk())) {
      LOG_WARN("failed to compute final topk", K(ret));
    } else {
      is_data_ready_ = true;
      output_idx_ = 0;
    }
  }
  return ret;
}

int ObHybridFusionOp::get_next_batch_knn_only(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);

  if (!is_data_ready_) {
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      const ObBatchRows *child_brs = nullptr;
      bool is_iter_end = false;
      if (OB_FAIL(fetch_next_child_batch(batch_cnt, child_brs, is_iter_end))) {
        LOG_WARN("failed to fetch next child batch in knn-only fast path", K(ret));
        break;
      } else if (OB_ISNULL(child_brs)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child batch is null in knn-only fast path", K(ret), K(is_iter_end));
      } else if (0 == child_brs->size_) {
        if (!is_iter_end) {
          continue;
        }
        break;
      } else if (OB_FAIL(store_batch_rows_knn_only(child_brs))) {
        LOG_WARN("failed to store child batch in knn-only fast path", K(ret));
      } else {
        if (is_iter_end) {
          break;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(prepare_output_knn_materialization())) {
      LOG_WARN("failed to prepare knn-only output materialization", K(ret));
    }
  }
  return ret;
}

int ObHybridFusionOp::get_next_batch_query_and_knn(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  bool has_passthrough_output = false;

  if (OB_UNLIKELY(search_index_ < 0 || path_count_ <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query_and_knn path requires search_index >= 0 and at least 2 paths", K(ret), K(path_count_), K(search_index_));
  } else if (PHASE_STREAMING == full_recall_phase_) {
    bool has_output = false;
    while (OB_SUCC(ret) && !has_output) {
      clear_evaluated_flag();
      const ObBatchRows *child_brs = nullptr;
      bool is_iter_end = false;
      if (OB_FAIL(fetch_next_child_batch(batch_cnt, child_brs, is_iter_end))) {
        break;
      } else if (OB_ISNULL(child_brs)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child batch is null in query+knn path", K(ret), K(is_iter_end));
      } else if (0 == child_brs->size_) {
        // PX / exchange may emit size==0 batches at partition boundaries without global EOS.
        enter_output_knn_phase_if_needed(is_iter_end);
        if (!is_iter_end) {
          continue;
        } else {
          break;
        }
      } else {
        ObExpr *query_score_expr = nullptr;
        bool has_query_path = false;
        int64_t child_output_cnt = 0;
        brs_.size_ = child_brs->size_;
        brs_.skip_->reset(child_brs->size_);
        if (OB_FAIL(get_query_score_expr(query_score_expr, has_query_path, child_output_cnt))) {
          LOG_WARN("failed to resolve search score expr in query+knn path", K(ret));
        } else if (OB_FAIL(prepare_batch_doc_indices(child_brs->size_))) {
          LOG_WARN("failed to prepare batch doc indices in query+knn path", K(ret), K(child_brs->size_));
        } else if (OB_FAIL(classify_child_batch_rows(
                       child_brs, query_score_expr, has_query_path, child_output_cnt, has_output))) {
          LOG_WARN("failed to classify child rows in query+knn path", K(ret), K(child_brs->size_));
        } else {
          if (OB_FAIL(push_scores_to_path_heaps(child_brs, child_output_cnt))) {
            LOG_WARN("failed to push scores to path heaps in query+knn path", K(ret), K(child_output_cnt));
          } else {
            enter_output_knn_phase_if_needed(is_iter_end);
            has_passthrough_output = has_output;
            brs_.end_ = has_passthrough_output ? child_brs->end_ : false;
            if (has_passthrough_output && PHASE_OUTPUT_KNN == full_recall_phase_) {
              brs_.end_ = false;
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && !has_passthrough_output && PHASE_OUTPUT_KNN == full_recall_phase_) {
    if (OB_FAIL(prepare_output_knn_materialization())) {
      LOG_WARN("failed to prepare query+knn output materialization", K(ret));
    }
  }

  if (OB_SUCC(ret) && !has_passthrough_output && PHASE_STREAMING == full_recall_phase_) {
    brs_.size_ = 0;
    brs_.end_ = true;
    brs_.reset_skip(0);
    brs_.set_all_rows_active(false);
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObHybridFusionOp::fetch_next_child_batch(const int64_t batch_cnt,
                                             const ObBatchRows *&child_brs,
                                             bool &is_iter_end)
{
  int ret = OB_SUCCESS;
  is_iter_end = false;
  if (OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next batch from child", K(ret));
    } else {
      ret = OB_SUCCESS;
      is_iter_end = true;
    }
  } else if (child_brs->end_) {
    is_iter_end = true;
  }
  return ret;
}

void ObHybridFusionOp::enter_output_knn_phase_if_needed(const bool is_iter_end)
{
  if (is_iter_end) {
    int ret = OB_SUCCESS;
    const FullRecallPhase old_phase = full_recall_phase_;
    // SKIP_FUSION single-path FTS etc.: hits are streamed in PHASE_STREAMING; row_store stays
    // empty. Entering PHASE_OUTPUT_KNN runs empty output_row_batch -> OB_ITER_END and breaks
    // vectorized parents unless normalized in emit_output_batch.
    if (row_store_.get_row_cnt() > 0) {
      full_recall_phase_ = PHASE_OUTPUT_KNN;
    }
  }
}

int ObHybridFusionOp::get_query_score_expr(ObExpr *&query_score_expr,
                                                bool &has_query_path,
                                                int64_t &child_output_cnt)
{
  int ret = OB_SUCCESS;
  query_score_expr = nullptr;
  // path_count_ is score_slots - 1 (trailing slot is fusion total score). Pure KNN uses search_index_ = -1.
  // When only one score column exists in the child output, path_count_ is 0 but search_index_ may still be 0
  // (FTS path); require search_index_ < path_count_ would wrongly drop all rows for COUNT(*).
  const int64_t n_score_slots = spec_.score_expr_output_indices_.count();
  has_query_path =
      search_index_ >= 0 && search_index_ < (n_score_slots <= 1 ? n_score_slots : n_score_slots - 1);
  child_output_cnt = child_->get_spec().output_.count();
  if (has_query_path) {
    const int64_t search_score_idx = spec_.score_expr_output_indices_.at(search_index_);
    if (OB_UNLIKELY(search_score_idx < 0 || search_score_idx >= child_output_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search score output index is invalid", K(ret), K(search_index_), K(search_score_idx),
               K(path_count_), K(child_output_cnt), K(spec_.score_expr_output_indices_));
    } else {
      query_score_expr = child_->get_spec().output_.at(search_score_idx);
    }
  }
  return ret;
}

int ObHybridFusionOp::prepare_batch_doc_indices(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  batch_doc_indices_.reuse();
  if (OB_FAIL(batch_doc_indices_.prepare_allocate(batch_size))) {
    LOG_WARN("failed to allocate batch_doc_indices", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_doc_indices_.count(); ++i) {
    batch_doc_indices_.at(i) = -1;
  }
  return ret;
}

int ObHybridFusionOp::classify_child_batch_rows(const ObBatchRows *child_brs,
                                                ObExpr *query_score_expr,
                                                const bool has_query_path,
                                                const int64_t child_output_cnt,
                                                bool &has_output)
{
  int ret = OB_SUCCESS;
  ObExpr *fusion_score_expr = nullptr;
  if (OB_UNLIKELY(spec_.score_expr_output_indices_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score_expr_output_indices_ is empty", K(ret));
  } else {
    const int64_t fusion_score_output_idx =
        spec_.score_expr_output_indices_.at(spec_.score_expr_output_indices_.count() - 1);
    if (OB_UNLIKELY(fusion_score_output_idx < 0 ||
                    fusion_score_output_idx >= child_->get_spec().output_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fusion_score_output_idx out of range", K(ret), K(fusion_score_output_idx),
               K(child_->get_spec().output_.count()));
    } else {
      fusion_score_expr = child_->get_spec().output_.at(fusion_score_output_idx);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(search_index_ < 0 || path_count_ <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("classify_child_batch_rows requires knn + query", K(ret), K(path_count_), K(search_index_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; ++i) {
    if (child_brs->skip_->at(i)) {
      brs_.set_skip(i);
      continue;
    }
    bool is_query_hit = false;
    bool has_knn_score = false;
    if (has_query_path) {
      if (MY_SPEC.use_rich_format_) {
        ObIVector *query_vec = query_score_expr->get_vector(eval_ctx_);
        is_query_hit = (query_vec != nullptr && !query_vec->is_null(i));
      } else {
        ObDatum *query_datums = query_score_expr->locate_batch_datums(eval_ctx_);
        is_query_hit = (query_datums != nullptr && !query_datums[i].is_null());
      }
    }

    for (int64_t path_idx = 0; OB_SUCC(ret) && !has_knn_score && path_idx < path_count_; ++path_idx) {
      if (path_idx == search_index_) {
        continue;
      }
      const int64_t knn_score_idx = spec_.score_expr_output_indices_.at(path_idx);
      if (OB_UNLIKELY(knn_score_idx < 0 || knn_score_idx >= child_output_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("knn score output index is invalid", K(ret), K(path_idx), K(knn_score_idx),
                 K(path_count_), K(child_output_cnt), K(spec_.score_expr_output_indices_));
      } else {
        ObExpr *knn_score_expr = child_->get_spec().output_.at(knn_score_idx);
        if (MY_SPEC.use_rich_format_) {
          ObIVector *knn_vec = knn_score_expr->get_vector(eval_ctx_);
          has_knn_score = (knn_vec != nullptr && !knn_vec->is_null(i));
        } else {
          ObDatum *knn_datums = knn_score_expr->locate_batch_datums(eval_ctx_);
          has_knn_score = (knn_datums != nullptr && !knn_datums[i].is_null());
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (!is_query_hit && !has_knn_score) {
        brs_.set_skip(i);
      } else if (is_query_hit) {
        double fusion_score = 0.0;
        if (MY_SPEC.use_rich_format_) {
          ObIVector *vec = fusion_score_expr->get_vector(eval_ctx_);
          if (OB_NOT_NULL(vec) && !vec->is_null(i)) {
            fusion_score = vec->get_double(i);
          }
        } else {
          ObDatum *datums = fusion_score_expr->locate_batch_datums(eval_ctx_);
          if (OB_NOT_NULL(datums) && !datums[i].is_null()) {
            fusion_score = datums[i].get_double();
          }
        }
        if (fusion_score < min_score_) {
          brs_.set_skip(i);
        } else {
          has_output = true;
          if (has_knn_score) {
            int64_t doc_idx = fusion_docs_.count();
            batch_doc_indices_.at(i) = doc_idx;
            ObFusionDocInfo doc_info;
            doc_info.fusion_score_ = 0.0;
            doc_info.row_store_idx_ = -1;
            if (OB_FAIL(fusion_docs_.push_back(doc_info))) {
              LOG_WARN("failed to push fusion doc for both-hit", K(ret));
            }
          }
        }
      } else {
        brs_.set_skip(i);
        ObCompactRow *stored_row = nullptr;
        if (OB_FAIL(row_store_.add_row(child_->get_spec().output_, i, eval_ctx_, stored_row))) {
          LOG_WARN("failed to store knn-only row", K(ret));
        } else {
          int64_t row_store_idx = row_store_.get_row_cnt() - 1;
          int64_t doc_idx = fusion_docs_.count();
          batch_doc_indices_.at(i) = doc_idx;
          ObFusionDocInfo doc_info;
          doc_info.fusion_score_ = 0.0;
          doc_info.row_store_idx_ = row_store_idx;
          if (OB_FAIL(fusion_docs_.push_back(doc_info))) {
            LOG_WARN("failed to push fusion doc for knn-only", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::push_scores_to_path_heaps(const ObBatchRows *child_brs,
                                                const int64_t child_output_cnt)
{
  int ret = OB_SUCCESS;
  for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count_; ++path_idx) {
    if (path_idx == search_index_) {
      continue;
    }
    const int64_t score_idx = spec_.score_expr_output_indices_.at(path_idx);
    if (OB_UNLIKELY(score_idx < 0 || score_idx >= child_output_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("path score output index is invalid", K(ret), K(path_idx), K(score_idx),
               K(path_count_), K(child_output_cnt), K(spec_.score_expr_output_indices_));
    } else {
      ObExpr *score_expr = child_->get_spec().output_.at(score_idx);
      ObTopKHeap *heap = path_heaps_.at(path_idx);
      int64_t top_k_limit = path_top_k_limit_.at(path_idx);
      if (top_k_limit <= 0) {
        continue;
      }
      if (MY_SPEC.use_rich_format_) {
        ObIVector *score_vec = score_expr->get_vector(eval_ctx_);
        if (OB_ISNULL(score_vec)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("score vec is null", K(ret), K(path_idx));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; ++i) {
            if (child_brs->skip_->at(i) || score_vec->is_null(i) || batch_doc_indices_.at(i) < 0) {
              continue;
            }
            double score = score_vec->get_double(i);
            int64_t doc_idx = batch_doc_indices_.at(i);
            if (heap->count() < top_k_limit || score > heap->top().score_) {
              if (OB_FAIL(((heap->count() < top_k_limit) ? heap->push(ObPathScoreEntry(doc_idx, score))
                                                         : heap->replace_top(ObPathScoreEntry(doc_idx, score))))) {
                LOG_WARN("failed to push to knn heap", K(ret));
              }
            }
          }
        }
      } else {
        ObDatum *datums = score_expr->locate_batch_datums(eval_ctx_);
        if (OB_ISNULL(datums)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("score datums is null", K(ret), K(path_idx));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; ++i) {
            if (child_brs->skip_->at(i) || datums[i].is_null() || batch_doc_indices_.at(i) < 0) {
              continue;
            }
            double score = datums[i].get_double();
            int64_t doc_idx = batch_doc_indices_.at(i);
            if (heap->count() < top_k_limit || score > heap->top().score_) {
              if (OB_FAIL(((heap->count() < top_k_limit) ? heap->push(ObPathScoreEntry(doc_idx, score))
                                                         : heap->replace_top(ObPathScoreEntry(doc_idx, score))))) {
                LOG_WARN("failed to push to knn heap", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::prepare_output_knn_materialization()
{
  int ret = OB_SUCCESS;
  if (!is_data_ready_) {
    if (OB_FAIL(rescore())) {
      LOG_WARN("failed to rescore in knn-only path", K(ret));
    } else if (OB_FAIL(compute_knn_only_topk())) {
      LOG_WARN("failed to compute knn only topk", K(ret));
    } else if (OB_FAIL(row_store_.finish_add_row(true))) {
      LOG_WARN("failed to finish add row", K(ret));
    } else {
      row_store_reader_.reset();
      if (OB_FAIL(row_store_reader_.init(&row_store_))) {
        LOG_WARN("failed to init row store reader", K(ret));
      } else {
        is_data_ready_ = true;
        output_idx_ = 0;
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::emit_output_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_FAIL(output_row_batch(max_row_cnt, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to output row batch", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObHybridFusionOp::compute_knn_only_topk()
{
  int ret = OB_SUCCESS;
  int64_t selected_doc_cnt = 0;
  int64_t skipped_doc_cnt = 0;
  const int64_t bucket_cnt = OB_MAX(path_count_ * OB_MAX(top_k_limit_, 1L), 1L);
  if (OB_FAIL(top_k_doc_indices_.create(bucket_cnt))) {
    LOG_WARN("failed to create top k doc indices", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < path_count_; ++i) {
      if (i == search_index_) {
        continue;
      }
      ObTopKHeap *heap = path_heaps_.at(i);
      const int64_t heap_size = heap->count();
      for (int64_t j = 0; OB_SUCC(ret) && j < heap_size; ++j) {
        const ObPathScoreEntry &entry = heap->at(j);
        if (OB_FAIL(top_k_doc_indices_.set_refactored(entry.doc_idx_))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to set doc index", K(ret), K(entry.doc_idx_));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (hash::ObHashSet<int64_t>::iterator iter = top_k_doc_indices_.begin();
         OB_SUCC(ret) && iter != top_k_doc_indices_.end();
         ++iter) {
      const int64_t doc_idx = iter->first;
      const ObFusionDocInfo &doc = fusion_docs_.at(doc_idx);
      if (doc.row_store_idx_ < 0) {
        ++skipped_doc_cnt;
        continue;
      }
      if (doc.fusion_score_ < min_score_) {
        ++skipped_doc_cnt;
        continue;
      }
      if (OB_FAIL(sorted_doc_indices_.push_back(doc_idx))) {
        LOG_WARN("failed to push knn-only doc index", K(ret), K(doc_idx));
      } else {
        ++selected_doc_cnt;
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::get_top_k_doc_indices()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_cnt = OB_MAX(path_count_ * OB_MAX(top_k_limit_, 1L), 1L);
  if (OB_FAIL(top_k_doc_indices_.create(bucket_cnt))) {
    LOG_WARN("failed to create top k doc indices", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < path_count_; ++i) {
      ObTopKHeap *heap = path_heaps_.at(i);
      const int64_t heap_size = heap->count();
      for (int64_t j = 0; OB_SUCC(ret) && j < heap_size; ++j) {
        const ObPathScoreEntry &entry = heap->at(j);
        if (OB_FAIL(top_k_doc_indices_.set_refactored(entry.doc_idx_))) {
          LOG_WARN("failed to set doc index", K(ret), K(entry.doc_idx_));
        }
      }
    }
  }
  return ret;
}

void ObHybridFusionOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  for (int64_t i = 0; i < path_heaps_.count(); ++i) {
    path_heaps_.at(i)->~ObTopKHeap();
  }
  path_heaps_.reset();
  comparers_.reset();
  weights_.reset();
  path_top_k_limit_.reset();
  fusion_docs_.reset();
  path_stats_.reset();
  sorted_doc_indices_.reset();
  row_store_.reset();
  top_k_doc_indices_.destroy();
  batch_doc_indices_.reset();
  passthrough_doc_indices_.reset();
  stored_rows_buffer_.reset();
  ObOperator::destroy();
}

int ObHybridFusionOp::init_path_heaps()
{
  int ret = OB_SUCCESS;
  ObIAllocator &alloc = ctx_.get_allocator();
  if (OB_FAIL(comparers_.prepare_allocate(path_count_))) {
    LOG_WARN("failed to allocate comparers", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < path_count_; ++i) {
      ObTopKHeap *heap = OB_NEWx(ObTopKHeap, &alloc, comparers_.at(i), &alloc);
      if (OB_ISNULL(heap)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate heap memory", K(ret));
      } else if (OB_FAIL(path_heaps_.push_back(heap))) {
        LOG_WARN("failed to push heap to array", K(ret));
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::try_push_to_heaps(const ObBatchRows *child_brs, const int64_t start_row_store_idx)
{
  int ret = OB_SUCCESS;
  batch_doc_indices_.reuse();
  if (OB_FAIL(batch_doc_indices_.prepare_allocate(child_brs->size_))) {
    LOG_WARN("failed to allocate batch_doc_indices", K(ret));
  } else {
    for (int64_t i = 0; i < batch_doc_indices_.count(); ++i) {
      batch_doc_indices_.at(i) = -1;
    }
  }
  for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count_; ++path_idx) {
    int64_t score_expr_idx = spec_.score_expr_output_indices_.at(path_idx);
    ObExpr *score_expr = child_->get_spec().output_.at(score_expr_idx);
    ObTopKHeap *heap = path_heaps_.at(path_idx);
    int64_t top_k_limit = path_top_k_limit_.at(path_idx);
    if (MY_SPEC.use_rich_format_) {
      ObIVector *score_vec = nullptr;
      if (OB_ISNULL(score_vec = score_expr->get_vector(eval_ctx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get vector", K(ret));
      } else if (OB_FAIL(try_push_to_heaps_rich_format(child_brs, score_vec, heap, start_row_store_idx, top_k_limit))) {
        LOG_WARN("failed to push to heaps rich format", K(ret));
      }
    } else {
      ObDatum *datums = nullptr;
      if (OB_ISNULL(datums = score_expr->locate_batch_datums(eval_ctx_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to locate batch datums", K(ret));
      } else if (OB_FAIL(try_push_to_heaps_non_rich_format(child_brs, datums, heap, start_row_store_idx, top_k_limit))) {
        LOG_WARN("failed to push to heaps non rich format", K(ret));
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::try_push_to_heaps_non_rich_format(const ObBatchRows *child_brs,
                                                        const ObDatum *datums,
                                                        ObTopKHeap *heap,
                                                        int64_t stored_row_idx,
                                                        int64_t top_k_limit)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < child_brs->size_; ++i) {
    double score = 0.0;
    if (child_brs->skip_->at(i)) {
      continue;
    } else if (top_k_limit <= 0) {
      stored_row_idx++;
      continue;
    } else if (datums[i].is_null()) {
      stored_row_idx++;
      continue;
    } else if (OB_FALSE_IT(score = datums[i].get_double())) {
    } else if (heap->count() < top_k_limit || score > heap->top().score_) {
      if (OB_FAIL(add_top_k_info(heap, score, stored_row_idx, i, top_k_limit))) {
        LOG_WARN("failed to add top k info", K(ret));
      }
    }
    stored_row_idx++;
  }
  return ret;
}

int ObHybridFusionOp::try_push_to_heaps_rich_format(const ObBatchRows *child_brs,
                                                    const ObIVector *score_vec,
                                                    ObTopKHeap *heap,
                                                    int64_t stored_row_idx,
                                                    int64_t top_k_limit)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < child_brs->size_; ++i) {
    double score = 0;
    if (child_brs->skip_->at(i)) {
      continue;
    } else if (top_k_limit <= 0) {
      stored_row_idx++;
      continue;
    } else if (score_vec->is_null(i)) {
      stored_row_idx++;
      continue;
    } else if (OB_FALSE_IT(score = score_vec->get_double(i))) {
    } else if (heap->count() < top_k_limit || score > heap->top().score_) {
      if (OB_FAIL(add_top_k_info(heap, score, stored_row_idx, i, top_k_limit))) {
        LOG_WARN("failed to add top k info", K(ret));
      }
    }
    stored_row_idx++;
  }
  return ret;
}

int ObHybridFusionOp::add_top_k_info(ObTopKHeap *heap, double score, int64_t stored_row_idx, int64_t start_index, int64_t top_k_limit)
{
  int ret = OB_SUCCESS;
  int64_t doc_idx = -1;
  if (batch_doc_indices_.at(start_index) < 0) {
    doc_idx = fusion_docs_.count();
    batch_doc_indices_.at(start_index) = doc_idx;
    ObFusionDocInfo doc_info;
    doc_info.fusion_score_ = 0;
    doc_info.row_store_idx_ = stored_row_idx;
    if (OB_FAIL(fusion_docs_.push_back(doc_info))) {
      LOG_WARN("failed to push back doc_info", K(ret));
    }
  } else {
    doc_idx = batch_doc_indices_.at(start_index);
  }

  if (OB_SUCC(ret)) {
    if (heap->count() < top_k_limit) {
      if (OB_FAIL(heap->push(ObPathScoreEntry(doc_idx, score)))) {
        LOG_WARN("failed to push score to heap", K(ret));
      }
    } else if (score > heap->top().score_) {
      if (OB_FAIL(heap->replace_top(ObPathScoreEntry(doc_idx, score)))) {
        LOG_WARN("failed to replace top score in heap", K(ret));
      }
    }
  }

  return ret;
}

int ObHybridFusionOp::store_batch_rows(const ObBatchRows *child_brs)
{
  int ret = OB_SUCCESS;
  int64_t stored_rows_count = 0;
  int64_t start_row_store_idx = row_store_.get_row_cnt();
  if (OB_FAIL(row_store_.add_batch(
              child_->get_spec().output_,
              eval_ctx_,
              *child_brs,
              stored_rows_count,
              NULL,
              0))) {
    LOG_WARN("failed to add batch to store", K(ret));
  } else if (OB_FAIL(try_push_to_heaps(child_brs, start_row_store_idx))) {
    LOG_WARN("failed to push to heaps", K(ret));
  }
  return ret;
}

int ObHybridFusionOp::store_batch_rows_knn_only(const ObBatchRows *child_brs)
{
  int ret = OB_SUCCESS;
  int64_t stored_rows_count = 0;
  const int64_t child_output_cnt = child_->get_spec().output_.count();
  int64_t row_store_idx = row_store_.get_row_cnt();
  if (OB_FAIL(row_store_.add_batch(
              child_->get_spec().output_,
              eval_ctx_,
              *child_brs,
              stored_rows_count,
              NULL,
              0))) {
    LOG_WARN("failed to add batch to store in knn-only path", K(ret));
  } else if (OB_FAIL(prepare_batch_doc_indices(child_brs->size_))) {
    LOG_WARN("failed to prepare batch doc indices in knn-only path", K(ret), K(child_brs->size_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; ++i) {
      if (child_brs->skip_->at(i)) {
        continue;
      } else {
        ObFusionDocInfo doc_info;
        doc_info.fusion_score_ = 0.0;
        doc_info.row_store_idx_ = row_store_idx++;
        batch_doc_indices_.at(i) = fusion_docs_.count();
        if (OB_FAIL(fusion_docs_.push_back(doc_info))) {
          LOG_WARN("failed to push knn-only doc info", K(ret), K(i));
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(push_scores_to_path_heaps(child_brs, child_output_cnt))) {
    LOG_WARN("failed to push scores to path heaps in knn-only path", K(ret), K(child_output_cnt));
  }
  return ret;
}

/**
 * @brief Collect all data from child operator (row-by-row execution) - memory optimized version
 *
 * Core optimization: Only create ObFusionDocInfo for documents that can enter Top-K heap
 *
 * Implementation:
 * 1. Extract scores from each path into temporary arrays
 * 2. For each new document, check if it can enter any Top-K heap
 * 3. Only create ObFusionDocInfo and store row data for documents that can enter heap
 * 4. Existing documents (previously entered heap) continue to update their scores
 *
 * Memory benefit:
 * - fusion_docs_.count() = candidate doc count (at most path_count * window_size)
 * - Instead of all scanned documents (could be millions)
 */

int ObHybridFusionOp::collect_all_data_batch()
{
  int ret = OB_SUCCESS;
  const int64_t max_batch_size = eval_ctx_.max_batch_size_;
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    const ObBatchRows *child_brs = nullptr;
    if (OB_FAIL(child_->get_next_batch(max_batch_size, child_brs))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next batch from child", K(ret));
      } else if (child_brs->size_ > 0) {
        if (OB_FAIL(store_batch_rows(child_brs))) {
          LOG_WARN("failed to store batch rows", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_FAIL(store_batch_rows(child_brs))) {
      LOG_WARN("failed to store batch rows", K(ret));
    } else if (child_brs->end_ || child_brs->size_ == 0) {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_store_.finish_add_row(true))) {
      LOG_WARN("failed to finish add row to store", K(ret));
    }
  }
  return ret;
}

int ObHybridFusionOp::rescore()
{
  int ret = OB_SUCCESS;
  ObFusionMethod fusion_method = spec_.fusion_method_;
  if (fusion_method == ObFusionMethod::RRF) {
    if (OB_FAIL(rescore_by_rrf())) {
      LOG_WARN("failed to calculate rrf score", K(ret));
    }
  } else if (fusion_method == ObFusionMethod::MINMAX_NORMALIZER) {
    if (OB_FAIL(get_min_max_score())) {
      LOG_WARN("failed to calculate normalizer score", K(ret));
    } else if (OB_FAIL(rescore_by_minmax())) {
      LOG_WARN("failed to calculate minmax score", K(ret));
    }
  } else if (fusion_method == ObFusionMethod::WEIGHT_SUM) {
    if (OB_FAIL(rescore_by_weight_sum())) {
      LOG_WARN("failed to calculate weight sum score", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported fusion method", K(ret), K(fusion_method));
  }
  return ret;
}

int ObHybridFusionOp::rescore_by_weight_sum()
{
  int ret = OB_SUCCESS;
  if (spec_.fusion_method_ != ObFusionMethod::WEIGHT_SUM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected fusion method", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < path_count_; ++i) {
    ObTopKHeap *heap = path_heaps_.at(i);
    double weight = weights_.at(i);
    const int64_t heap_size = heap->count();
    for (int64_t j = 0; OB_SUCC(ret) && j < heap_size; ++j) {
      const ObPathScoreEntry &entry = heap->at(j);
      if (entry.doc_idx_ < 0 || entry.doc_idx_ >= fusion_docs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid doc_idx", K(ret), K(entry.doc_idx_), K(fusion_docs_.count()));
      } else {
        ObFusionDocInfo& doc_info = fusion_docs_.at(entry.doc_idx_);
        double old_score = doc_info.fusion_score_;
        doc_info.fusion_score_ += entry.score_ * weight;
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::get_min_max_score()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(path_stats_.prepare_allocate(path_count_))) {
    LOG_WARN("failed to prepare allocate path_stats", K(ret), K(path_count_));
  }

  for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count_; ++path_idx) {
    ObPathStats &stats = path_stats_.at(path_idx);
    stats.reset();

    if (path_idx >= path_heaps_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("heap not initialized or null", K(ret), K(path_idx), K(path_heaps_.count()));
    } else {
      ObTopKHeap *heap = path_heaps_.at(path_idx);
      const int64_t heap_size = heap->count();
      if (heap_size == 0) {
        stats.min_score_ = 0.0;
        stats.max_score_ = 0.0;
      } else {
        double min_score = heap->at(0).score_;
        double max_score = heap->at(0).score_;
        for (int64_t i = 1; i < heap_size; ++i) {
          double score = heap->at(i).score_;
          if (score < min_score) {
            min_score = score;
          }
          if (score > max_score) {
            max_score = score;
          }
        }
        stats.min_score_ = min_score;
        stats.max_score_ = max_score;
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::rescore_by_minmax()
{
  int ret = OB_SUCCESS;
  if (spec_.fusion_method_ != ObFusionMethod::MINMAX_NORMALIZER) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected normalizer type", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < path_count_; ++i) {
    ObTopKHeap *heap = path_heaps_.at(i);
    double min_score = path_stats_.at(i).min_score_;
    double max_score = path_stats_.at(i).max_score_;
    double weight = weights_.at(i);
    bool min_equal_max = (max_score - min_score) < EPSILON;
    const int64_t heap_size = heap->count();
    for (int64_t j = 0; OB_SUCC(ret) && j < heap_size; ++j) {
      const ObPathScoreEntry &entry = heap->at(j);
      if (entry.doc_idx_ < 0 || entry.doc_idx_ >= fusion_docs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid doc_idx", K(ret), K(entry.doc_idx_), K(fusion_docs_.count()));
      } else {
        ObFusionDocInfo& doc_info = fusion_docs_.at(entry.doc_idx_);
        double normalized_score = 0.0;
        if (min_equal_max) {
          normalized_score = 1.0;
        } else {
          normalized_score = (entry.score_ - min_score) / (max_score - min_score);
        }
        doc_info.fusion_score_ += normalized_score * weight;
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::rescore_by_rrf()
{
  int ret = OB_SUCCESS;
  if (spec_.fusion_method_ != ObFusionMethod::RRF) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected fusion method", K(ret));
  }
  // Min-heap: top element has smallest score (worst rank = size)
  // Pop from top to bottom, rank from size to 1 (rank 1 is the best)
  for (int64_t i = 0; OB_SUCC(ret) && i < path_count_; ++i) {
    ObTopKHeap *heap = path_heaps_.at(i);
    double weight = weights_.at(i);
    int64_t rank_constant = rank_constant_;

    // Extract all entries from heap (sorted from lowest to highest score)
    ObSEArray<ObPathScoreEntry, 16> entries;
    while (OB_SUCC(ret) && heap->count() > 0) {
      if (OB_FAIL(entries.push_back(heap->top()))) {
        LOG_WARN("failed to push entry", K(ret));
      } else if (OB_FAIL(heap->pop())) {
        LOG_WARN("failed to pop from heap", K(ret));
      }
    }

    // Traverse in reverse order (highest score to lowest score)
    // Best score gets rank 1, same score gets same rank
    double last_score = 0.0;
    uint64_t current_rank = 0;
    bool is_first = true;
    int64_t total_count = entries.count();
    for (int64_t j = total_count - 1; OB_SUCC(ret) && j >= 0; --j) {
      const ObPathScoreEntry &entry = entries.at(j);
      // Same score should have the same rank
      if (is_first || entry.score_ != last_score) {
        current_rank = total_count - j;  // rank starts from 1 for the highest score
        last_score = entry.score_;
        is_first = false;
      }
      if (entry.doc_idx_ < 0 || entry.doc_idx_ >= fusion_docs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid doc_idx", K(ret), K(entry.doc_idx_), K(fusion_docs_.count()));
      } else {
        ObFusionDocInfo& doc_info = fusion_docs_.at(entry.doc_idx_);
        // RRF: 1 / (k + rank)
        double rrf_score = 1.0 / (rank_constant + current_rank);
        doc_info.fusion_score_ += rrf_score * weight;
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::compute_fusion_topk()
{
  int ret = OB_SUCCESS;
  int64_t topk = top_k_limit_;
  for (hash::ObHashSet<int64_t>::iterator iter = top_k_doc_indices_.begin(); OB_SUCC(ret) && iter != top_k_doc_indices_.end(); ++iter) {
    int64_t doc_idx = iter->first;
    const ObFusionDocInfo &doc = fusion_docs_.at(doc_idx);
    if (doc.fusion_score_ >= min_score_) {
      if (OB_FAIL(sorted_doc_indices_.push_back(doc_idx))) {
        LOG_WARN("failed to push doc index", K(ret), K(doc_idx));
      }
    }
  }
  if (OB_SUCC(ret) && sorted_doc_indices_.count() > 0) {
    struct DocIndexCompare {
      const common::ObSEArray<ObFusionDocInfo, 10> &fusion_docs_;
      DocIndexCompare(const common::ObSEArray<ObFusionDocInfo, 10> &docs)
        : fusion_docs_(docs) {}
      bool operator()(const int64_t lhs, const int64_t rhs) const {
        return fusion_docs_.at(lhs).fusion_score_ > fusion_docs_.at(rhs).fusion_score_;
      }
    };
    DocIndexCompare cmp(fusion_docs_);
    int64_t *begin = sorted_doc_indices_.get_data();
    if (OB_ISNULL(begin)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sorted_doc_indices_ data is null", K(ret));
    } else {
      lib::ob_sort(begin, begin + sorted_doc_indices_.count(), cmp);
      topk = top_k_limit_ > sorted_doc_indices_.count() ? sorted_doc_indices_.count() : top_k_limit_;
      while (sorted_doc_indices_.count() > topk) {
        sorted_doc_indices_.pop_back();
      }
      if (spec_.fusion_iter_exec_mode_ == ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS) {
        const int64_t total_count = sorted_doc_indices_.count();
        const int64_t start = OB_MIN(offset_, total_count);
        int64_t end = total_count;
        if (size_ >= 0) {
          end = OB_MIN(start + size_, total_count);
        }
        const int64_t new_count = end - start;
        for (int64_t i = 0; i < new_count; ++i) {
          sorted_doc_indices_[i] = sorted_doc_indices_[start + i];
        }
        while (sorted_doc_indices_.count() > new_count) {
          sorted_doc_indices_.pop_back();
        }
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::get_store_row_batch(int64_t batch_size, const ObCompactRow **&stored_rows)
{
  int ret = OB_SUCCESS;
  if (batch_size > stored_rows_buffer_.get_capacity()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("batch_size exceeds stored_rows_buffer capacity", K(ret), K(batch_size), K(stored_rows_buffer_.get_capacity()));
  } else {
    stored_rows = stored_rows_buffer_.get_data();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    int64_t doc_idx = sorted_doc_indices_.at(output_idx_ + i);
    const ObFusionDocInfo &doc = fusion_docs_.at(doc_idx);
    int64_t row_store_idx = doc.row_store_idx_;
    if (OB_FAIL(row_store_reader_.get_row(row_store_idx, stored_rows[i]))) {
      LOG_WARN("failed to get row from store", K(ret), K(row_store_idx));
    }
  }
  return ret;
}

int ObHybridFusionOp::output_row_rich_format(int64_t batch_size, const ObCompactRow **stored_rows)
{
  int ret = OB_SUCCESS;
  int64_t fusion_score_output_idx = spec_.score_expr_output_indices_.at(spec_.score_expr_output_indices_.count() - 1);
  ObExpr *fusion_score_expr = child_->get_spec().output_.at(fusion_score_output_idx);
  for (int64_t i= 0; OB_SUCC(ret) && i < child_->get_spec().output_.count(); ++i) {
    ObExpr *expr = child_->get_spec().output_.at(i);
    ObIVector *vec = nullptr;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(i));
    } else if (OB_FAIL(expr->init_vector_for_write(eval_ctx_, expr->get_default_res_format(), batch_size))) {
      LOG_WARN("failed to init vector", K(ret), K(i));
    } else if (OB_ISNULL(vec = expr->get_vector(eval_ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vec is null", K(ret), K(i));
    } else if (OB_FAIL(vec->from_rows(row_store_.get_row_meta(), stored_rows, batch_size, i))) {
      LOG_WARN("failed to convert rows to vector", K(ret), K(i));
    } else if (expr == fusion_score_expr) {
      for (int64_t j = 0; j < batch_size; ++j) {
        int64_t doc_idx = sorted_doc_indices_.at(output_idx_ + j);
        const ObFusionDocInfo &doc = fusion_docs_.at(doc_idx);
        vec->set_double(j, doc.fusion_score_);
      }
    }
    if (OB_SUCC(ret)) {
      expr->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

int ObHybridFusionOp::output_row_non_rich_format(int64_t batch_size, const ObCompactRow **stored_rows)
{
  int ret = OB_SUCCESS;
  int64_t fusion_score_output_idx = spec_.score_expr_output_indices_.at(spec_.score_expr_output_indices_.count() - 1);
  ObExpr *fusion_score_expr = child_->get_spec().output_.at(fusion_score_output_idx);
  for (int64_t i = 0; OB_SUCC(ret) && i < child_->get_spec().output_.count(); ++i) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    batch_info_guard.set_batch_size(batch_size);
    ObExpr *expr = child_->get_spec().output_.at(i);
    ObDatum *datums = expr->locate_datums_for_update(eval_ctx_, batch_size);
    if (OB_ISNULL(datums)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vec is null", K(ret), K(i));
    } else if (expr == fusion_score_expr) {
      for (int64_t j = 0; j < batch_size; ++j) {
        int64_t doc_idx = sorted_doc_indices_.at(output_idx_ + j);
        const ObFusionDocInfo &doc = fusion_docs_.at(doc_idx);
        datums[j].set_double(doc.fusion_score_);
      }
    } else {
      const RowMeta &row_meta = row_store_.get_row_meta();
      for (int64_t j = 0; j < batch_size; ++j) {
        if (OB_ISNULL(stored_rows[j])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("stored_rows is null", K(ret), K(j));
        } else if (stored_rows[j]->is_null(i)) {
          datums[j].set_null();
        } else {
          datums[j] = stored_rows[j]->get_datum(row_meta, i);
        }
      }
      if (OB_SUCC(ret)) {
        child_->get_spec().output_.at(i)->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObHybridFusionOp::output_row_batch(const int64_t max_row_cnt, int64_t count)
{
  int ret = OB_SUCCESS;
  const int64_t total_count = sorted_doc_indices_.count();
  const int64_t batch_size = OB_MIN(max_row_cnt, total_count - output_idx_);
  const ObCompactRow **stored_rows = nullptr;
  int64_t fusion_score_output_idx = spec_.score_expr_output_indices_.at(spec_.score_expr_output_indices_.count() - 1);
  ObExpr *fusion_score_expr = child_->get_spec().output_.at(fusion_score_output_idx);
  if (output_idx_ >= total_count) {
    brs_.size_ = 0;
    brs_.end_ = true;
    brs_.reset_skip(0);
    brs_.set_all_rows_active(false);
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_store_row_batch(batch_size, stored_rows))) {
    LOG_WARN("failed to get store row batch", K(ret), K(batch_size));
  } else if (MY_SPEC.use_rich_format_) {
    if (OB_FAIL(output_row_rich_format(batch_size, stored_rows))) {
      LOG_WARN("failed to output row rich format", K(ret), K(batch_size));
    }
  } else {
    if (OB_FAIL(output_row_non_rich_format(batch_size, stored_rows))) {
      LOG_WARN("failed to output row non rich format", K(ret), K(batch_size));
    }
  }
  if (OB_SUCC(ret)) {
    const bool old_all_rows_active = brs_.all_rows_active_;
    const int64_t old_skip_cnt = nullptr == brs_.skip_ ? -1 : brs_.skip_->accumulate_bit_cnt(batch_size);
    brs_.reset_skip(batch_size);
    count = batch_size;
    output_idx_ += batch_size;
    brs_.size_ = batch_size;
    brs_.end_ = false;
    if (output_idx_ >= total_count) {
      brs_.end_ = true;
    }
  }
  return ret;
}

int ObHybridFusionOp::init_constant_params()
{
  int ret = OB_SUCCESS;
  path_count_ = spec_.score_expr_output_indices_.count() - 1;
  fusion_iter_exec_mode_ = spec_.fusion_iter_exec_mode_;
  search_index_ = spec_.search_index_;
  ObDatum *size_datum = nullptr;
  ObDatum *rank_window_size_datum = nullptr;
  ObDatum *offset_datum = nullptr;
  ObDatum *min_score_datum = nullptr;
  if (OB_FAIL(spec_.size_expr_->eval(eval_ctx_, size_datum))) {
    LOG_WARN("failed to eval size expr", K(ret));
  } else if (OB_ISNULL(size_datum) || size_datum->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("size datum is null", K(ret));
  } else if (OB_FAIL(spec_.rank_window_size_expr_->eval(eval_ctx_, rank_window_size_datum))) {
    LOG_WARN("failed to eval rank window size expr", K(ret));
  } else if (OB_ISNULL(rank_window_size_datum) || rank_window_size_datum->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rank window size datum is null", K(ret));
  } else if (OB_FAIL(spec_.offset_expr_->eval(eval_ctx_, offset_datum))) {
    LOG_WARN("failed to eval offset expr", K(ret));
  } else if (OB_ISNULL(offset_datum) || offset_datum->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset datum is null", K(ret));
  } else if (OB_FAIL(spec_.min_score_expr_->eval(eval_ctx_, min_score_datum))) {
    LOG_WARN("failed to eval min score expr", K(ret));
  } else if (OB_ISNULL(min_score_datum) || min_score_datum->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min score datum is null", K(ret));
  } else {
    size_ = size_datum->get_int();
    top_k_limit_ = rank_window_size_datum->get_int();
    offset_ = offset_datum->get_int();
    min_score_ = min_score_datum->get_double();
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < spec_.weights_exprs_.count(); ++i) {
      double weight = 1.0;
      ObDatum *weight_datum = nullptr;
      if (OB_ISNULL(spec_.weights_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("weight expr is null", K(ret), K(i));
      } else if (OB_FAIL(spec_.weights_exprs_.at(i)->eval(eval_ctx_, weight_datum))) {
        LOG_WARN("failed to eval weight", K(ret));
      } else if (OB_ISNULL(weight_datum) || weight_datum->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("weight datum is null", K(ret));
      } else if (OB_FALSE_IT(weight = weight_datum->get_double())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get weight", K(ret));
      } else if (OB_FAIL(weights_.push_back(weight))) {
        LOG_WARN("failed to push back weight", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < spec_.path_top_k_limit_exprs_.count(); ++i) {
      int64_t path_top_k_value = 0;
      ObDatum *path_top_k_value_datum = nullptr;
      if (OB_ISNULL(spec_.path_top_k_limit_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("path top k limit expr is null", K(ret), K(i));
      } else if (OB_FAIL(spec_.path_top_k_limit_exprs_.at(i)->eval(eval_ctx_, path_top_k_value_datum))) {
        LOG_WARN("failed to eval path top k value", K(ret));
      } else if (OB_ISNULL(path_top_k_value_datum) || path_top_k_value_datum->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("path top k value datum is null", K(ret));
      } else if (OB_FALSE_IT(path_top_k_value = path_top_k_value_datum->get_int())) {
      } else if (OB_FAIL(path_top_k_limit_.push_back(path_top_k_value))) {
        LOG_WARN("failed to push back path top k value", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && spec_.fusion_method_ == ObFusionMethod::RRF) {
    ObDatum *rank_constant_datum = nullptr;
    if (OB_ISNULL(spec_.rank_constant_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rank constant expr is null", K(ret));
    } else if (OB_FAIL(spec_.rank_constant_expr_->eval(eval_ctx_, rank_constant_datum))) {
      LOG_WARN("failed to eval rank constant", K(ret));
    } else if (OB_ISNULL(rank_constant_datum) || rank_constant_datum->is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rank constant datum is null", K(ret));
    } else {
      rank_constant_ = rank_constant_datum->get_int();
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase