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
#include "storage/tx_storage/ob_access_service.h"
#include "storage/access/ob_table_scan_range.h"
#include "share/ob_simple_batch.h"

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

    ObDASIndexMergeFTSAndIterParam &fts_and_iter_param = static_cast<ObDASIndexMergeFTSAndIterParam&>(param);
    if (OB_FAIL(relevance_exprs_.assign(fts_and_iter_param.relevance_exprs_))) {
      LOG_WARN("failed to assign relevance exprs", K(ret));
    } else {
      limit_ = fts_and_iter_param.limit_;
      offset_ = fts_and_iter_param.offset_;
      pushdown_topk_iter_ = fts_and_iter_param.pushdown_topk_iter_;
      pushdown_topk_iter_tree_ = fts_and_iter_param.pushdown_topk_iter_tree_;
      pushdown_topk_iter_idx_ = fts_and_iter_param.pushdown_topk_iter_idx_;

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

int ObDASIndexMergeFTSAndIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASIndexMergeAndIter::do_table_scan())) {
    LOG_WARN("failed to do table scan", K(ret));
  } else if (OB_FAIL(determine_execute_strategy())) {
    LOG_WARN("failed to determine execute strategy", K(ret));
  } else if (SECOND_PHASE_ONLY == execute_strategy_) {
    pushdown_topk_iter_->set_topk_limit(pushdown_topk_);
    if (OB_FAIL(pushdown_topk_iter_tree_->do_table_scan())) {
      LOG_WARN("failed to do table scan", K(ret));
    } else {
      pushdown_topk_iter_first_scan_ = false;
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
  } else if (SECOND_PHASE_ONLY == execute_strategy_) {
    if (OB_FAIL(pushdown_topk_iter_tree_->reuse())) {
      LOG_WARN("failed to reuse pushdown topk iter tree", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ready_to_output_ = false;
    cur_result_item_idx_ = 0;
    result_item_size_ = 0;
  }
  result_items_ = nullptr;

  return ret;
}

int ObDASIndexMergeFTSAndIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASIndexMergeAndIter::rescan())) {
    LOG_WARN("failed to rescan index merge and iter", K(ret));
  } else if (OB_FAIL(determine_execute_strategy())) {
    LOG_WARN("failed to determine execute strategy", K(ret));
  } else if (SECOND_PHASE_ONLY == execute_strategy_) {
    if (!pushdown_topk_iter_first_scan_) {
      if (OB_FAIL(pushdown_topk_iter_->adjust_topk_limit(pushdown_topk_))) {
        LOG_WARN("failed to reprepare pushdown topk", K(ret));
      } else if (OB_FAIL(pushdown_topk_iter_tree_->rescan())) {
        LOG_WARN("failed to rescan pushdown topk iter tree", K(ret));
      }
    } else {
      pushdown_topk_iter_->set_topk_limit(pushdown_topk_);
      if (OB_FAIL(pushdown_topk_iter_tree_->do_table_scan())) {
        LOG_WARN("failed to rescan pushdown topk iter tree", K(ret));
      } else {
        pushdown_topk_iter_first_scan_ = false;
      }
    }
  }
  return ret;
}

int ObDASIndexMergeFTSAndIter::estimate_child_index_selectivity(int64_t child_idx, double &selectivity)
{
  int ret = OB_SUCCESS;
  selectivity = 1.0; // Default to 1.0

  int64_t logical_row_cnt = 0;
  int64_t physical_row_cnt = 0;
  ObSEArray<ObEstRowCountRecord, 1> est_records;
  ObArenaAllocator est_allocator;
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  ObAccessService *access_service = MTL(ObAccessService *);
  storage::ObTableScanRange table_scan_range;

  ObDASScanRtDef *child_scan_rtdef = child_scan_rtdefs_.at(child_idx);
  const ObDASScanCtDef *child_scan_ctdef = static_cast<const ObDASScanCtDef*>(child_scan_rtdef->ctdef_);

  if (OB_ISNULL(child_scan_ctdef) || OB_ISNULL(child_scan_rtdef) || OB_ISNULL(access_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(child_scan_ctdef), KP(child_scan_rtdef), KP(access_service), K(child_idx));
  } else if (child_scan_rtdef->key_ranges_.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null key ranges", K(ret), K(child_idx));
  } else {
    ObTableScanParam *child_scan_param = get_child_scan_param(child_idx);

    // Step 1: Estimate filtered row count
    ObTableScanParam est_param;
    est_param.index_id_ = child_scan_ctdef->ref_table_id_;
    est_param.scan_flag_ = child_scan_rtdef->scan_flag_;
    est_param.tablet_id_ = child_tablet_ids_.at(child_idx);
    est_param.ls_id_ = ls_id_;
    est_param.tx_id_ = tx_desc_->get_tx_id();
    est_param.schema_version_ = child_scan_ctdef->schema_version_;
    est_param.frozen_version_ = GET_BATCH_ROWS_READ_SNAPSHOT_VERSION;

    ObSimpleBatch batch;
    batch.type_ = ObSimpleBatch::T_SCAN;
    batch.range_ = &child_scan_param->key_ranges_.at(0);

    if (OB_FAIL(table_scan_range.init(*child_scan_param, batch, est_allocator))) {
      STORAGE_LOG(WARN, "Failed to init table scan range", K(ret), K(batch));
    } else if (OB_FAIL(access_service->estimate_row_count(est_param,
                                                          table_scan_range,
                                                          timeout_us,
                                                          est_records,
                                                          logical_row_cnt,
                                                          physical_row_cnt))) {
      LOG_TRACE("OPT:[STORAGE EST FAILED]", "storage_ret", ret, K(child_idx));
    } else {
      // Step 2: Get total row count
      int64_t macro_block_count = 0;
      int64_t micro_block_count = 0;
      int64_t sstable_row_count = 0;
      int64_t memtable_row_count = 0;
      ObSEArray<int64_t, 1> cg_macro_cnt_arr;
      ObSEArray<int64_t, 1> cg_micro_cnt_arr;

      if (OB_FAIL(access_service->estimate_block_count_and_row_count(ls_id_,
                                                                     child_tablet_ids_.at(child_idx),
                                                                     timeout_us,
                                                                     macro_block_count,
                                                                     micro_block_count,
                                                                     sstable_row_count,
                                                                     memtable_row_count,
                                                                     cg_macro_cnt_arr,
                                                                     cg_micro_cnt_arr))) {
        LOG_TRACE("Failed to get total row count", K(ret), K(child_idx));
      } else {
        // Step 3: Calculate selectivity
        int64_t total_row_count = sstable_row_count + memtable_row_count;
        if (total_row_count > 0) {
          selectivity = static_cast<double>(logical_row_cnt) / static_cast<double>(total_row_count);
          // Ensure selectivity is in [0, 1]
          selectivity = std::max(0.0, std::min(1.0, selectivity));
        }
      }
    }
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::determine_execute_strategy()
{
  int ret = OB_SUCCESS;

  const double FORCE_NORMAL_INDEX_MERGE_SELECTIVITY = 0.3;
  execute_strategy_ = SECOND_PHASE_ONLY;
  double min_selectivity = 1.0;

  for (int64_t child_idx = 0; OB_SUCC(ret) && child_idx < relevance_exprs_.count(); ++child_idx) {
    if (nullptr == relevance_exprs_.at(child_idx)) {  // index merge scan node
      double selectivity = 1.0;
      if (OB_FAIL(estimate_child_index_selectivity(child_idx, selectivity))) {
        LOG_WARN("failed to estimate child index selectivity", K(ret), K(child_idx));
      } else if (selectivity <= FORCE_NORMAL_INDEX_MERGE_SELECTIVITY) {
        execute_strategy_ = NORMAL_INDEX_MERGE;
        min_selectivity = selectivity;
        break;
      } else if (selectivity < min_selectivity) {
        min_selectivity = selectivity;
      }
    }
  }

  if (SECOND_PHASE_ONLY == execute_strategy_) {
    pushdown_topk_ = (limit_ + offset_) / min_selectivity;
  }

  LOG_DEBUG("two phase fts index merge execute strategy", K(ret), K(execute_strategy_), K(min_selectivity), K(pushdown_topk_));
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
  } else if (OB_NOT_NULL(pushdown_topk_iter_tree_) && OB_FAIL(pushdown_topk_iter_tree_->release())) {
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
    if (OB_FAIL(execute_second_round_scan(is_vectorized, capacity, first_round_results))) {
      LOG_WARN("failed to execute second round scan", K(ret));
    } else if (OB_FAIL(sort_first_round_result_by_relevance(first_round_results, result_items_))) {
      LOG_WARN("failed to sort first round result by relevance", K(ret));
    } else {
      cur_result_item_idx_ = 0;
      ready_to_output_ = true;
    }
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (SECOND_PHASE_ONLY == execute_strategy_) {
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
  } else if (NORMAL_INDEX_MERGE == execute_strategy_) {
    if (OB_FAIL(ObDASIndexMergeAndIter::inner_get_next_rows(count, capacity))) {
      LOG_WARN("failed to get next rows", K(ret));
    }
  }

  return ret;
}

int ObDASIndexMergeFTSAndIter::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObDASIndexMergeFTSAndIter::fill_other_child_stores(int64_t capacity)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < fts_index_idxs_.count(); i++) {
    if (fts_index_idxs_.at(i) == pushdown_topk_iter_idx_) {
      // skip
    } else if (OB_FAIL(fill_one_child_store(capacity, fts_index_idxs_.at(i), &relevance_exprs_))) {
      LOG_WARN("failed to fill one child stores", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < normal_index_idxs_.count(); i++) {
    if (OB_FAIL(fill_one_child_store(capacity, normal_index_idxs_.at(i), nullptr))) {
      LOG_WARN("failed to fill one child stores", K(ret));
    }
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

int ObDASIndexMergeFTSAndIter::execute_second_round_scan(bool is_vectorized, int64_t capacity, ObMinRelevanceHeap &first_round_results)
{
  int ret = OB_SUCCESS;
  int64_t actual_top_n = 0;
  ObMinRelevanceHeap fts_result_heap(pushdown_topk_, mem_ctx_->get_arena_allocator(), relevance_cmp_);
  ObFTSResultItem* sorted_fts_result_items;

  if (!is_vectorized && OB_FAIL(get_topn_fts_result(fts_result_heap))) {
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
      if (child_stores_.at(i).is_empty()) {
        child_stores_.at(i).reuse();
      }
    }

    if (OB_FAIL(fill_other_child_stores(capacity))) {
      LOG_WARN("failed to fill child stores", K(ret));
    } else if (child_empty_count_ > 0) {
      ret = OB_ITER_END;
    } else {
      int64_t child_cnt = child_stores_.count();
      while (OB_SUCC(ret) && child_empty_count_ <= 0 && fts_row_idx < actual_top_n) {
        int64_t last_child_store_idx = OB_INVALID_INDEX;
        ObFTSResultItem &item = sorted_fts_result_items[fts_row_idx];
        MergeResultBuffer *row_buffer = item.row_buffer_;
        int cmp_ret = 0;
        for (int64_t i = 0; OB_SUCC(ret) && (cmp_ret == 0) &&
                            child_empty_count_ <= 0 &&
                            i < child_cnt && fts_row_idx < actual_top_n; i++) {
          if (i == pushdown_topk_iter_idx_) {
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
              } else if (cmp_ret > 0) {
                last_child_store.incre_idx();
              }
            }
          }
        } // end for

        if (OB_SUCC(ret) && cmp_ret == 0) {
          for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
            if (i == pushdown_topk_iter_idx_) {
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
  IndexMergeRowStore &child_store = child_stores_.at(pushdown_topk_iter_idx_);

  while (OB_SUCC(ret) && child_empty_count_ <= 0) {
    clear_evaluated_flag();
    pushdown_topk_iter_tree_->clear_evaluated_flag();
    child_store.reuse();

    if (OB_FAIL(fill_one_child_store(capacity, pushdown_topk_iter_idx_, &relevance_exprs_, pushdown_topk_iter_tree_))) {
      LOG_WARN("failed to fill child stores", K(ret));
    } else if (child_empty_count_ > 0) {
      ret = OB_ITER_END;
    } else {
      while (OB_SUCC(ret) && child_empty_count_ <= 0) {
        while (!child_store.is_empty() && child_store.get_relevance() == 0.0) {
          child_store.incre_idx();
        }

        double relevance = 0;
        if (child_store.is_empty()) {
          ret = OB_ITER_END;
        } else if (OB_FALSE_IT(relevance = child_store.get_relevance())) {
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
