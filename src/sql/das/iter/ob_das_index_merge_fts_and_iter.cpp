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
#include "sql/das/iter/ob_das_index_merge_fts_and_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASIndexMergeFTSAndIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObDASIndexMergeAndIter::inner_init(param))) {
    LOG_WARN("failed to init index merge and iter", K(ret));
  } else {
    ready_to_output_ = 0;
    cur_result_item_idx_ = 0;
    result_item_size_ = 0;
    result_items_ = nullptr;
    first_round_min_relevance_ = 0;

    ObDASIndexMergeFTSAndIterParam &fts_and_iter_param = static_cast<ObDASIndexMergeFTSAndIterParam&>(param);
    if (OB_FAIL(relevance_exprs_.assign(fts_and_iter_param.relevance_exprs_))) {
      LOG_WARN("failed to assign relevance exprs", K(ret));
    } else {
      limit_ = fts_and_iter_param.limit_;
      offset_ = fts_and_iter_param.offset_;
      pushdown_topk_iter_ = fts_and_iter_param.pushdown_topk_iter_;
      pushdown_topk_iter_tree_ = fts_and_iter_param.pushdown_topk_iter_tree_;
      first_fts_idx_ = fts_and_iter_param.first_fts_idx_;

      for (int64_t i = 0; OB_SUCC(ret) && i < relevance_exprs_.count(); i++) {
        ObDASIter *child = child_iters_.at(i);
        ObIArray<uint64_t> *target_idxs = nullptr;
        ObIArray<ObExpr*> *target_output_exprs = nullptr;
        if (nullptr == relevance_exprs_.at(i)) {
          target_idxs = &normal_index_idxs_;
          target_output_exprs = &normal_index_output_exprs_;
        } else {
          target_idxs = &fts_index_idxs_;
          target_output_exprs = &fts_index_output_exprs_;
        }
        if (OB_FAIL(target_idxs->push_back(i))) {
          LOG_WARN("failed to push back index idx", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < child->get_output()->count(); j++) {
            if (OB_FAIL(target_output_exprs->push_back(child->get_output()->at(j)))) {
              LOG_WARN("failed to push back output expr", K(ret));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::inner_reuse()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObDASIndexMergeAndIter::inner_reuse())) {
    LOG_WARN("failed to reuse index merge and iter", K(ret));
  } else if (OB_FAIL(result_buffer_.reuse())) {
    LOG_WARN("failed to reuse result buffer", K(ret));
  } else if (!pushdown_topk_iter_first_scan_ && OB_FAIL(pushdown_topk_iter_tree_->reuse())) {
    LOG_WARN("failed to reuse pushdown topk iter tree", K(ret));
  } else {
    ready_to_output_ = false;
    cur_result_item_idx_ = 0;
    result_item_size_ = 0;
    first_round_min_relevance_ = 0;
  }
  result_items_ = nullptr;

  return ret;
}

int ObDASIndexMergeFTSAndIter::inner_release()
{
  int ret = OB_SUCCESS;

  fts_index_idxs_.reset();
  normal_index_idxs_.reset();
  relevance_exprs_.reset();
  fts_index_output_exprs_.reset();
  normal_index_output_exprs_.reset();

  if (OB_FAIL(ObDASIndexMergeAndIter::inner_release())) {
    LOG_WARN("failed to release index merge and iter", K(ret));
  } else if (OB_FAIL(pushdown_topk_iter_tree_->release())) {
    LOG_WARN("failed to release pushdown topk iter tree", K(ret));
  } else {
    result_items_ = nullptr;
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::prepare_outout(bool is_vectorized, int64_t capacity) {
  int ret = OB_SUCCESS;

  if (ready_to_output_) {
    // do nothing
  } else{
    int64_t limit_k = limit_ + offset_;
    ObMinRelevanceHeap first_round_results(limit_k, mem_ctx_->get_arena_allocator(), relevance_cmp_);

    if (is_vectorized && OB_FAIL(execute_first_round_scan_vectorized(capacity, first_round_results))) {
      LOG_WARN("failed to execute first round scan vectorized", K(ret));
    } else if (!is_vectorized && OB_FAIL(execute_first_round_scan(first_round_results))) {
      LOG_WARN("failed to execute first round scan", K(ret));
    } else if (first_round_results.count() >= limit_k && OB_FAIL(execute_second_round_scan(is_vectorized, capacity, first_round_results))) {
      LOG_WARN("failed to execute second round scan", K(ret));
    } else if (OB_FAIL(sort_first_round_result_by_relevance(first_round_results, result_items_))) {
      LOG_WARN("failed to sort first round result by relevance", K(ret));
    } else {
      cur_result_item_idx_ = offset_;
      ready_to_output_ = true;
    }
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (!ready_to_output_ && OB_FAIL(prepare_outout(true, capacity))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to prepare output", K(ret));
    }
  } else if (cur_result_item_idx_ < result_item_size_) {
    ObFTSResultItem &result_item = result_items_[cur_result_item_idx_];
    if (OB_FAIL(result_item.row_buffer_->to_expr(1))) {
      LOG_WARN("failed to to expr", K(ret));
    } else {
      cur_result_item_idx_++;
      count = 1;
    }
  } else {
    ret = OB_ITER_END;
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;

  if (!ready_to_output_ && OB_FAIL(prepare_outout(false, 1))) {
    LOG_WARN("failed to prepare output", K(ret));
  } else if (cur_result_item_idx_ < result_item_size_) {
    ObFTSResultItem &result_item = result_items_[cur_result_item_idx_];
    if (OB_FAIL(result_item.row_buffer_->to_expr(1))) {
      LOG_WARN("failed to to expr", K(ret));
    } else {
      cur_result_item_idx_++;
    }
  } else {
    ret = OB_ITER_END;
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::fill_other_child_stores(int64_t capacity)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < fts_index_idxs_.count(); i++) {
    if (fts_index_idxs_.at(i) == first_fts_idx_) {
      // skip
    } else if (OB_FAIL(fill_one_child_stores(capacity, fts_index_idxs_.at(i), child_iters_.at(fts_index_idxs_.at(i))))) {
      LOG_WARN("failed to fill one child stores", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < normal_index_idxs_.count(); i++) {
    if (OB_FAIL(fill_one_child_stores(capacity, normal_index_idxs_.at(i), child_iters_.at(normal_index_idxs_.at(i))))) {
      LOG_WARN("failed to fill one child stores", K(ret));
    }
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::fill_one_child_stores(int64_t capacity, int64_t child_idx, ObDASIter *child_iter)
{
  int ret = OB_SUCCESS;

  IndexMergeRowStore &child_store = child_stores_.at(child_idx);
  if (child_store.iter_end_) {
    // no more rows for child iter
  } else {
    int64_t child_count = 0;
    int64_t child_capacity = std::min(capacity, child_store.capacity_ - child_store.count());
    if (OB_ISNULL(child_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(child_idx));
    } else if (OB_UNLIKELY(child_capacity <= 0)) {
      // do nothing
    } else {
      if (OB_FAIL(child_iter->get_next_rows(child_count, child_capacity))) {
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
          child_store.iter_end_ = true;
          iter_end_count_ ++;
        } else {
          LOG_WARN("failed to get next rows from child iter", K(ret), K(capacity), K(child_count), K(child_capacity));
        }
      } else if (OB_UNLIKELY(child_count <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child count is invalid", K(ret), K(child_count));
      }

      ObExpr *relevance_expr = relevance_exprs_.at(child_idx);
      if (OB_FAIL(ret) || child_count <= 0) {
      } else if (OB_FAIL(child_store.save(true, child_count, relevance_expr))) {
        LOG_WARN("failed to save child rows", K(ret), K(child_count));
      } else if (child_iter->get_type() == DAS_ITER_SORT) {
        reset_datum_ptr(child_iter->get_output(), child_count);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (child_store.iter_end_ && !child_store.drained_ && child_store.is_empty()) {
    child_empty_count_ ++;
    child_store.drained_ = true;
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::execute_first_round_scan_vectorized(int64_t capacity, ObMinRelevanceHeap &first_round_results)
{
  int ret = OB_SUCCESS;
  int64_t saved_cnt = 0;

  int64_t limit_k = limit_+offset_;
  while (OB_SUCC(ret) && child_empty_count_ <= 0 && saved_cnt < limit_k) {
    clear_evaluated_flag();
    if (OB_FAIL(fill_child_stores(capacity, &relevance_exprs_))) {
      LOG_WARN("failed to fill child stores", K(ret));
    } else if (child_empty_count_ > 0) {
      ret = OB_ITER_END;
    } else {
      int64_t child_cnt = child_stores_.count();
      int64_t last_child_store_idx = OB_INVALID_INDEX;
      while (OB_SUCC(ret) && saved_cnt < limit_k) {
        int cmp_ret = 0;
        for (int64_t i = 0; OB_SUCC(ret) && (cmp_ret == 0) && i < child_cnt; i++) {
          IndexMergeRowStore &child_store = child_stores_.at(i);

          if (relevance_exprs_.at(i) != nullptr) {  // fts index skip data which relevance=0
            while (!child_store.is_empty() && child_store.get_relevance() == 0.0) {
              child_store.incre_idx();
            }
          }

          if (child_store.is_empty()) {
            ret = OB_ITER_END;
          } else if (last_child_store_idx == OB_INVALID_INDEX) {
            last_child_store_idx = i;
          } else if (last_child_store_idx == i) {
            // skip
          } else if (child_stores_.at(last_child_store_idx).is_empty()) {
            ret = OB_ITER_END;
          } else {
            IndexMergeRowStore &last_child_store = child_stores_.at(last_child_store_idx);
            if (OB_FAIL(ObDASIndexMergeIter::compare(child_store, last_child_store, cmp_ret))) {
              LOG_WARN("failed to compare row", K(ret), K(child_store), K(last_child_store));
            } else if (cmp_ret < 0) {
              child_store.incre_idx();
              last_child_store_idx = i;
            } else if (cmp_ret > 0) {
              last_child_store.incre_idx();
            }
          }
        } // end for

        if (OB_SUCC(ret) && cmp_ret == 0) {
          last_child_store_idx = OB_INVALID_INDEX;

          double cur_row_relevance = 0.0;
          for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
            IndexMergeRowStore &output_store = child_stores_.at(i);
            if (output_store.is_empty()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected empty store", K(ret), K(output_store));
            } else if (i == first_fts_idx_ && OB_FALSE_IT(cur_row_relevance = output_store.get_relevance())) {
            } else if (OB_FAIL(output_store.to_expr(1))) {
              LOG_WARN("failed to expr", K(ret), K(output_store));
            }
          }

          if (OB_FAIL(ret)) {
          } else {
            ObFTSResultItem result_item;
            result_item.relevance_ = cur_row_relevance;
            MergeResultBuffer* buffer = (MergeResultBuffer*)mem_ctx_->get_arena_allocator().alloc(sizeof(MergeResultBuffer));
            if (OB_ISNULL(buffer)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate buffer", K(ret));
            } else if (OB_FALSE_IT(new(buffer) MergeResultBuffer())){
            } else if (OB_FAIL(buffer->init(1, eval_ctx_, output_, mem_ctx_->get_arena_allocator()))) {
              LOG_WARN("failed to init buffer", K(ret));
            } else if (OB_FAIL(buffer->add_rows(1))) {
              LOG_WARN("failed to add rows", K(ret));
            } else if (OB_FALSE_IT(result_item.row_buffer_ = buffer)) {
            } else if (OB_FAIL(first_round_results.push(result_item))) {
              LOG_WARN("failed to push result item", K(ret));
            } else if (OB_FALSE_IT(saved_cnt ++)) {
            } else if (saved_cnt == 1 || cur_row_relevance < first_round_min_relevance_) {
              first_round_min_relevance_ = cur_row_relevance;
            }
          }
        }
      } // end while

      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::get_relevance(int64_t child_idx, double &relevance)
{
  int ret = OB_SUCCESS;

  if (child_idx < 0 || child_idx >= relevance_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child idx", K(ret), K(child_idx));
  } else {
    ObExpr* relevance_expr = relevance_exprs_.at(child_idx);
    ObEvalCtx *eval_ctx = child_stores_.at(child_idx).eval_ctx_;

    if (OB_ISNULL(relevance_expr) || OB_ISNULL(eval_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), K(child_idx));
    } else {
      const ObDatum &relevance_datum = relevance_expr->locate_expr_datum(*eval_ctx_);
      relevance = relevance_datum.get_double();
    }
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::set_topk_relevance_threshold()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(first_round_min_relevance_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relevance", K(ret), K(first_round_min_relevance_));
  } else if (pushdown_topk_iter_first_scan_) {
    if (OB_FAIL(pushdown_topk_iter_tree_->do_table_scan())) {
      LOG_WARN("failed to do table scan", K(ret));
    } else {
      pushdown_topk_iter_first_scan_ = false;
    }
  } else {
    // todo set_tablet_ids
    if (OB_FAIL(pushdown_topk_iter_tree_->rescan())) {
      LOG_WARN("failed to do table scan", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(pushdown_topk_iter_->preset_top_k_threshold(first_round_min_relevance_))) {
    LOG_WARN("failed to preset top k threshold", K(ret), K(first_round_min_relevance_));
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::execute_second_round_scan(bool is_vectorized, int64_t capacity, ObMinRelevanceHeap &first_round_results)
{
  int ret = OB_SUCCESS;
  int64_t actual_top_n = 0;
  int64_t top_n = (limit_ + offset_) * 2;  // todo: 暂时放大两倍
  ObMinRelevanceHeap fts_result_heap(top_n, mem_ctx_->get_arena_allocator(), relevance_cmp_);
  ObFTSResultItem* sorted_fts_result_items;

  if (OB_FAIL(set_topk_relevance_threshold())) {
    LOG_WARN("failed to set top k relevance threshold", K(ret));
  } else if (!is_vectorized && OB_FAIL(get_topn_fts_result(fts_result_heap))) {
    LOG_WARN("failed to get topn fts result", K(ret));
  } else if (is_vectorized && OB_FAIL(get_topn_fts_result_vectorized(capacity, fts_result_heap))) {
    LOG_WARN("failed to execute second round scan vectorized", K(ret));
  } else if (OB_FALSE_IT(actual_top_n = fts_result_heap.count())) {
  } else if (actual_top_n == 0) {
  } else if (OB_ISNULL(sorted_fts_result_items = (ObFTSResultItem *)mem_ctx_->get_arena_allocator().alloc(sizeof(ObFTSResultItem) * actual_top_n))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(actual_top_n));
  } else if (OB_FAIL(sort_fts_result_by_rowkey(fts_result_heap, sorted_fts_result_items))) {
    LOG_WARN("failed to sort fts result by rowkey", K(ret));
  } else if (!is_vectorized && OB_FAIL(filter_fts_result_by_other_index(first_round_results, sorted_fts_result_items, actual_top_n))) {
    LOG_WARN("failed to filter fts result by normal index", K(ret));
  } else if (is_vectorized && OB_FAIL(filter_fts_result_by_other_index_vectorized(capacity, first_round_results, sorted_fts_result_items, actual_top_n))) {
    LOG_WARN("failed to filter fts result by normal index vectorized", K(ret));
  }
  return ret;
}

int ObDASIndexMergeFTSAndIter::filter_fts_result_by_other_index_vectorized(
    int64_t capacity,
    ObMinRelevanceHeap &first_round_results,
    ObFTSResultItem* &sorted_fts_result_items,
    int64_t actual_top_n)
{
  int ret = OB_SUCCESS;
  int64_t fts_row_idx = 0;

  while (OB_SUCC(ret) && child_empty_count_ <= 0 && fts_row_idx < actual_top_n) {
    clear_evaluated_flag();
    for (int64_t i = 0; i < child_stores_.count(); i++) {
      child_stores_.at(i).reuse();
    }

    if (OB_FAIL(fill_other_child_stores(capacity))) {
      LOG_WARN("failed to fill child stores", K(ret));
    } else if (child_empty_count_ > 0) {
      ret = OB_ITER_END;
    } else {
      int64_t child_cnt = child_stores_.count();
      int64_t last_child_store_idx = OB_INVALID_INDEX;
      while (OB_SUCC(ret) && child_empty_count_ <= 0 && fts_row_idx < actual_top_n) {
        ObFTSResultItem &item = sorted_fts_result_items[fts_row_idx];
        MergeResultBuffer *row_buffer = item.row_buffer_;
        int cmp_ret = 0;
        for (int64_t i = 0; OB_SUCC(ret) && (cmp_ret == 0) &&
                            child_empty_count_ <= 0 &&
                            i < child_cnt && fts_row_idx < actual_top_n; i++) {
          if (i == first_fts_idx_) {
            // skip
          } else {
            IndexMergeRowStore &child_store = child_stores_.at(i);

            if (relevance_exprs_.at(i) != nullptr) {
              while (!child_store.is_empty() && child_store.get_relevance() == 0.0) {
                child_store.incre_idx();
              }
            }

            if (child_store.is_empty()) {
              ret = OB_ITER_END;
            } else if (last_child_store_idx == OB_INVALID_INDEX) {
              // first fts index
              if (OB_FAIL(compare(row_buffer, child_store, cmp_ret))) {
                LOG_WARN("failed to compare row", K(ret), K(child_store));
              } else if (cmp_ret == 0) {
                last_child_store_idx = i;
              } else if (cmp_ret < 0) {
                fts_row_idx++;
              } else if (cmp_ret > 0) {
                child_store.incre_idx();
              }
            } else if (last_child_store_idx == i) {
              // skip
            } else if (child_stores_.at(last_child_store_idx).is_empty()) {
              ret = OB_ITER_END;
            } else {
              IndexMergeRowStore &last_child_store = child_stores_.at(last_child_store_idx);
              if (OB_FAIL(ObDASIndexMergeIter::compare(child_store, last_child_store, cmp_ret))) {
                LOG_WARN("failed to compare row", K(ret), K(child_store), K(last_child_store));
              } else if (cmp_ret < 0) {
                child_store.incre_idx();
                last_child_store_idx = i;
              } else if (cmp_ret > 0) {
                last_child_store.incre_idx();
              }
            }
          }
        } // end for

        if (OB_SUCC(ret) && cmp_ret == 0) {
          for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
            if (i == first_fts_idx_) {
              if (OB_FAIL(item.row_buffer_->to_expr(1))) {
                LOG_WARN("failed to to expr", K(ret));
              }
            } else {
              IndexMergeRowStore &output_store = child_stores_.at(i);
              if (output_store.is_empty()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected empty store", K(ret), K(output_store));
              } else if (OB_FAIL(output_store.to_expr(1))) {
                LOG_WARN("failed to expr", K(ret), K(output_store));
              }
            }
          }

          if (OB_FAIL(ret)) {
          } else {
            ObFTSResultItem result_item;
            result_item.relevance_ = item.relevance_;
            MergeResultBuffer* buffer = (MergeResultBuffer*)mem_ctx_->get_arena_allocator().alloc(sizeof(MergeResultBuffer));
            if (OB_ISNULL(buffer)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate buffer", K(ret));
            } else if (OB_FALSE_IT(new(buffer) MergeResultBuffer())){
            } else if (OB_FAIL(buffer->init(1, eval_ctx_, output_, mem_ctx_->get_arena_allocator()))) {
              LOG_WARN("failed to init buffer", K(ret));
            } else if (OB_FAIL(buffer->add_rows(1))) {
              LOG_WARN("failed to add rows", K(ret));
            } else if (OB_FALSE_IT(result_item.row_buffer_ = buffer)) {
            } else if (OB_FAIL(first_round_results.push(result_item))) {
              LOG_WARN("failed to push result item", K(ret));
            } else {
              fts_row_idx++;
            }
          }
        }
      } // end while
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::get_topn_fts_result_vectorized(
    int64_t capacity,
    ObMinRelevanceHeap &heap)
{
  int ret = OB_SUCCESS;
  IndexMergeRowStore &child_store = child_stores_.at(first_fts_idx_);

  while (OB_SUCC(ret) && child_empty_count_ <= 0) {
    clear_evaluated_flag();
    pushdown_topk_iter_tree_->clear_evaluated_flag();
    child_store.reuse();

    if (OB_FAIL(fill_one_child_stores(capacity, first_fts_idx_, pushdown_topk_iter_tree_))) {
      LOG_WARN("failed to fill child stores", K(ret));
    } else if (child_empty_count_ > 0) {
      ret = OB_ITER_END;
    } else {
      while (OB_SUCC(ret) && child_empty_count_ <= 0) {
        while (!child_store.is_empty() && child_store.get_relevance() == 0.0) {
          child_store.incre_idx();
        }

        double relevance = child_store.get_relevance();
        if (child_store.is_empty()) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(child_store.to_expr(1))) {
          LOG_WARN("failed to expr", K(ret), K(child_store));
        } else {
          ObFTSResultItem result_item;
          result_item.relevance_ = relevance;
          MergeResultBuffer* buffer = (MergeResultBuffer*)mem_ctx_->get_arena_allocator().alloc(sizeof(MergeResultBuffer));
          if (OB_ISNULL(buffer)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate buffer", K(ret));
          } else if (OB_FALSE_IT(new(buffer) MergeResultBuffer())) {
          } else if (OB_FAIL(buffer->init(1, eval_ctx_, child_store.exprs_, mem_ctx_->get_arena_allocator()))) {
            LOG_WARN("failed to init buffer", K(ret));
          } else if (OB_FAIL(buffer->add_rows(1))) {
            LOG_WARN("failed to add rows", K(ret));
          } else if (OB_FALSE_IT(result_item.row_buffer_ = buffer)) {
          } else if (OB_FAIL(heap.push(result_item))) {
            LOG_WARN("failed to push result item", K(ret));
          }
        }
      }

      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  child_empty_count_ -= 1;

  return ret;
}

int ObDASIndexMergeFTSAndIter::sort_fts_result_by_rowkey(
  ObMinRelevanceHeap &heap,
    ObFTSResultItem* &fts_result_items)
{
  int ret = OB_SUCCESS;
  int64_t size = heap.count();

  for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
    fts_result_items[i] = heap.top();
    if (OB_FAIL(heap.pop())) {
      LOG_WARN("failed to pop heap", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObFTSResultItemRowkeyCmp comp(ret, rowkey_exprs_, is_reverse_);
    lib::ob_sort(fts_result_items, fts_result_items+size, comp);

    if (comp.ret_ != OB_SUCCESS) {
      ret = comp.ret_;
      LOG_WARN("failed to sort fts result by rowkey", K(ret));
    }
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::sort_first_round_result_by_relevance(
  ObMinRelevanceHeap &heap,
  ObFTSResultItem* &first_round_result_items)
{
  int ret = OB_SUCCESS;
  int64_t size = heap.count();
  result_item_size_ = size;

  if (size == 0) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(first_round_result_items = (ObFTSResultItem *)mem_ctx_->get_arena_allocator().alloc(sizeof(ObFTSResultItem) * size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
      first_round_result_items[size - i - 1] = heap.top();
      if (OB_FAIL(heap.pop())) {
        LOG_WARN("failed to pop heap", K(ret));
      }
    }
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::compare(MergeResultBuffer* row_buffer, IndexMergeRowStore &cmp_store, int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if (OB_UNLIKELY(cmp_store.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cmp_store));
  } else if (OB_ISNULL(row_buffer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row_buffer is nullptr", K(ret));
  } else {
    const ObChunkDatumStore::StoredRow *sr = NULL;
    const ObChunkDatumStore::StoredRow *cmp_row = cmp_store.first_row();
    if (OB_ISNULL(cmp_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), KPC(this), K(cmp_row));
    } else if (OB_FAIL(row_buffer->result_store_iter_.get_cur_row(sr))) {
      LOG_WARN("failed to get next row from row buffer", K(ret));
    } else {
      const ObDatum *cur_datums = sr->cells();
      const ObDatum *cmp_datums = cmp_row->cells();
      if (OB_ISNULL(cur_datums) || OB_ISNULL(cmp_datums)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(cur_datums), K(cmp_datums));
      } else if (OB_UNLIKELY(cur_datums->is_null() || cmp_datums->is_null())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null datum", K(ret), KPC(cur_datums), KPC(cmp_datums));
      } else {
        ObObj cur_obj;
        ObObj cmp_obj;
        for (int64_t i = 0; (cmp_ret == 0) && OB_SUCC(ret) && i < rowkey_exprs_->count(); i++) {
          const ObExpr *expr = rowkey_exprs_->at(i);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", K(ret));
          } else if (OB_FAIL(cur_datums[i].to_obj(cur_obj, expr->obj_meta_, expr->obj_datum_map_))) {
            LOG_WARN("failed to convert left datum to obj", K(i), KPC(expr), K(ret));
          } else if (OB_FAIL(cmp_datums[i].to_obj(cmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
            LOG_WARN("failed to convert right datum to obj", K(i), KPC(expr), K(ret));
          } else if (OB_FAIL(cur_obj.check_collation_free_and_compare(cmp_obj, cmp_ret))) {
            LOG_WARN("failed to compare cur obj with output obj", K(ret));
          } else if (OB_FALSE_IT(cmp_ret = OB_UNLIKELY(is_reverse_) ? -cmp_ret : cmp_ret)) {
          }
        } // end for
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
