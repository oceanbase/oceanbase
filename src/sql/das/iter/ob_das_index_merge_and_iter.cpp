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
#include "sql/das/iter/ob_das_index_merge_and_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/ob_das_attach_define.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASIndexMergeAndIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (ObDASIterType::DAS_ITER_INDEX_MERGE != param.type_ && ObDASIterType::DAS_ITER_TWO_PHASE_INDEX_MERGE != param.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param));
  } else {
    ObDASIndexMergeIterParam &index_merge_param = static_cast<ObDASIndexMergeIterParam&>(param);
    if (index_merge_param.merge_type_ != INDEX_MERGE_INTERSECT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index merge iter param with bad merge type", K(index_merge_param.merge_type_));
    } else if (OB_FAIL(ObDASIndexMergeIter::inner_init(param))) {
      LOG_WARN("inner init index merge and iter failed", K(param));
    } else if (index_merge_param.main_scan_iter_ != nullptr && OB_FAIL(prepare_main_scan_param(index_merge_param.main_scan_iter_))) {
      LOG_WARN("failed to prepare main scan param", K(ret));
    } else if (OB_FAIL(check_can_be_shorted())) {
      LOG_WARN("index merge iter failed to check can be shorted", K(ret));
    } else {
      shorted_child_idx_ = OB_INVALID_INDEX;
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
      enable_bitmap_cursor_ = (tenant_config.is_valid() ? tenant_config->_enable_index_merge_intersect_bitmap : false) &&
                              rowkey_is_uint64_ &&
                              !is_reverse_ &&
                              eval_ctx_->is_vectorized();
      if (OB_FAIL(init_child_cursors())) {
        LOG_WARN("failed to init child cursors", K(ret));
      }
    }
  }
  return ret;
}

int ObDASIndexMergeAndIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_cursors_.count(); ++i) {
    if (OB_ISNULL(child_cursors_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr cursor", K(ret), K(i));
    } else {
      child_cursors_.at(i)->reuse();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDASIndexMergeIter::inner_reuse())) {
      LOG_WARN("index merge iter failed to reuse", K(ret));
    }
  }
  if (main_scan_iter_ != nullptr) {
    const ObTabletID &new_tablet_id = main_scan_tablet_id_;
    const ObTabletID &old_tablet_id = main_scan_param_->tablet_id_;
    main_scan_param_->need_switch_param_ = main_scan_param_->need_switch_param_ ||
        (old_tablet_id.is_valid() && old_tablet_id != new_tablet_id);
    if (OB_FAIL(main_scan_iter_->reuse())) {
      LOG_WARN("main scan iter failed to reuse", K(ret));
    }
  }
  if (OB_NOT_NULL(lookup_memctx_)) {
    lookup_memctx_->reset_remain_one_page();
  }
  shorted_child_idx_ = OB_INVALID_INDEX;
  return ret;
}

int ObDASIndexMergeAndIter::inner_release()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < child_cursors_.count(); ++i) {
    if (OB_NOT_NULL(child_cursors_.at(i))) {
      child_cursors_.at(i)->reset();
    }
  }
  child_cursors_.reset();
  if (OB_FAIL(ObDASIndexMergeIter::inner_release())) {
    LOG_WARN("inner release index merge or iter failed");
  }
  if (main_scan_iter_ != nullptr) {
    if (OB_FAIL(main_scan_iter_->release())) {
      LOG_WARN("main scan iter failed to release", K(ret));
    }
  }
  if (OB_NOT_NULL(main_scan_param_)) {
    main_scan_param_->destroy_schema_guard();
    main_scan_param_->snapshot_.reset();
    main_scan_param_->destroy();
    main_scan_param_ = nullptr;
  }
  if (OB_NOT_NULL(lookup_memctx_)) {
    lookup_memctx_->reset_remain_one_page();
    DESTROY_CONTEXT(lookup_memctx_);
    lookup_memctx_ = nullptr;
  }
  shorted_child_idx_ = OB_INVALID_INDEX;
  return ret;
}

int ObDASIndexMergeAndIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(sort_get_next_row())) {
    LOG_WARN("index merge iter failed to get next row", K(ret));
  } else if (child_empty_count_ > 0) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObDASIndexMergeAndIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (main_scan_iter_ == nullptr) {
    if (OB_FAIL(get_next_merge_rows(count, capacity))) {
      LOG_WARN("failed to get next merge rows", K(ret), K(count), K(capacity));
    }
  } else {
    // need use main scan to calc non ror filters
    bool got_rows = false;
    while (OB_SUCC(ret) && !got_rows) {
      if (OB_FAIL(get_next_merge_rows(count, capacity))) {
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          LOG_WARN("failed to get next merge rows", K(ret), K(count), K(capacity));
        } else {
          ret = count > 0 ? OB_SUCCESS : OB_ITER_END;
        }
      }
      if (OB_SUCC(ret) && count > 0) {
        // reuse main scan iter and release memory of query range
        main_scan_param_->need_switch_param_ = false;
        main_scan_iter_->reuse();
        lookup_memctx_->reset_remain_one_page();
        main_scan_param_->ls_id_ = ls_id_;
        main_scan_param_->tablet_id_ = main_scan_tablet_id_;
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
        batch_info_guard.set_batch_size(count);
        for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
          batch_info_guard.set_batch_idx(i);
          if (OB_FAIL(main_scan_iter_->set_scan_rowkey(eval_ctx_, *rowkey_exprs_, merge_ctdef_->main_scan_ctdef_, &lookup_memctx_->get_arena_allocator(), 0))) {
            LOG_WARN("failed to set scan rowkey", K(ret));
          }
        }
        if (first_main_scan_) {
          if (OB_FAIL(main_scan_iter_->do_table_scan())) {
            LOG_WARN("failed to do table scan", K(ret));
          } else {
            first_main_scan_ = false;
          }
        } else if (OB_FAIL(main_scan_iter_->rescan())) {
          LOG_WARN("failed to rescan", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(main_scan_iter_->get_next_rows(count, capacity))) {
          if (OB_UNLIKELY(ret != OB_ITER_END)) {
            LOG_WARN("failed to get next rows", K(ret), K(count), K(capacity));
          } else {
            ret = OB_SUCCESS;
          }
        }
        if (OB_SUCC(ret) && count > 0) {
          got_rows = true;
        }
      }
    }
    LOG_TRACE("lookup for non-ror filters get next rows", K(ret), K(count), K(capacity), KPC(main_scan_iter_));
  }
  return ret;
}

int ObDASIndexMergeAndIter::get_next_merge_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  while (OB_SUCC(ret) && child_empty_count_ <= 0 && count < capacity) {
    clear_evaluated_flag();
    if (OB_FAIL(fill_child_stores(capacity))) {
      LOG_WARN("fill child stores failed", K(ret), K(child_empty_count_));
    } else if (enable_bitmap_cursor_ && OB_FAIL(check_child_cursors())) {
      LOG_WARN("failed to check cursors", K(ret));
    } else if (child_empty_count_ > 0) {
      ret = OB_ITER_END;
    } else if (can_be_shorted_ && iter_end_count_ > 0 && force_merge_mode_ == 0) {
      // it ends early and do rescan directly when a child has iter end
      LOG_TRACE("shorted path", K(can_be_shorted_), K(iter_end_count_), K(child_empty_count_), K(count), K(capacity));
      if (OB_FAIL(shorted_get_next_rows(count, capacity))) {
        LOG_WARN("index merge iter failed to get next row by shorted", K(ret), K(count), K(capacity));
      }
    } else if (OB_FAIL(sort_get_next_rows(count, capacity))) {
      LOG_WARN("index merge iter failed to get next row by sort", K(ret), K(count), K(capacity));
    }
  } // end while

  if (OB_FAIL(ret)) {
    if (OB_LIKELY(ret == OB_ITER_END)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(result_buffer_rows_to_expr(count))) {
        ret = tmp_ret;
        LOG_WARN("output rows to expr failed", K(ret), K(count), K(capacity));
      }
    }
  } else if (count == 0 && child_empty_count_ > 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(result_buffer_rows_to_expr(count))) {
    LOG_WARN("output all row to expr failed", K(ret), K(count), K(capacity));
  }
  const ObBitVector *skip = nullptr;
  PRINT_VECTORIZED_ROWS(SQL, DEBUG, *eval_ctx_, *output_, count, skip);

  return ret;
}

int ObDASIndexMergeAndIter::prepare_main_scan_param(ObDASScanIter *main_scan_iter)
{
  int ret = OB_SUCCESS;
  ObTableScanParam *main_scan_param = nullptr;
  lib::ContextParam context_param;
  context_param.set_mem_attr(MTL_ID(), "NonRorLookup", ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  if (OB_ISNULL(main_scan_iter) ||
      OB_ISNULL(main_scan_param = OB_NEWx(ObTableScanParam, &mem_ctx_->get_arena_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table scan param", K(ret));
  } else if (OB_FAIL(init_scan_param(
      ls_id_, main_scan_tablet_id_, merge_ctdef_->main_scan_ctdef_, merge_rtdef_->main_scan_rtdef_, *main_scan_param))) {
    LOG_WARN("failed to init scan param", K(ret));
  } else if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(lookup_memctx_, context_param))) {
    LOG_WARN("failed to create non ror lookup memctx", K(ret));
  } else {
    main_scan_param->is_get_ = true;
    main_scan_param_ = main_scan_param;
    main_scan_iter->set_scan_param(*main_scan_param_);
    main_scan_iter_ = main_scan_iter;
  }
  LOG_TRACE("prepare main scan param", K(ret), KPC(main_scan_param_));
  return ret;
}

int ObDASIndexMergeAndIter::shorted_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObDASIndexMergeAndIter::shorted_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && shorted_child_idx_ == OB_INVALID_INDEX && i < child_stores_.count(); i++) {
    IndexMergeRowStore &child_store = child_stores_.at(i);
    if (child_store.iter_end_) {
      // we just need to output the first child which gets iter end
      shorted_child_idx_ = i;
    }
  }

  if (OB_UNLIKELY(shorted_child_idx_ == OB_INVALID_INDEX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no child store iter end", K(ret));
  } else {
    IndexMergeRowStore &child_store = child_stores_.at(shorted_child_idx_);
    int64_t output_row_cnt = std::min(child_store.count(), capacity - count);
    if (OB_UNLIKELY(output_row_cnt == 0)) {
      // do nothing
    } else if (OB_FAIL(child_store.to_expr(output_row_cnt))) {
      LOG_WARN("failed to expr", K(ret), K(shorted_child_idx_), K(child_store), K(output_row_cnt), K(capacity));
    } else if (OB_FAIL(save_row_to_result_buffer(output_row_cnt))) {
      LOG_WARN("failed to save row to result buffer", K(ret), K(shorted_child_idx_), K(output_row_cnt));
    } else {
      count += output_row_cnt;
    }
  }
  return ret;
}

int ObDASIndexMergeAndIter::sort_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  int64_t child_cnt = child_stores_.count();
  int64_t last_child_store_idx = OB_INVALID_INDEX;
  while (OB_SUCC(ret) && child_empty_count_ <= 0 && !got_row) {
    int cmp_ret = 0;
    for (int64_t i = 0; OB_SUCC(ret) && (cmp_ret == 0) && child_empty_count_ <= 0 && i < child_cnt; i++) {
      IndexMergeRowStore &child_store = child_stores_.at(i);
      if (child_store.is_empty()) {
        if (OB_UNLIKELY(child_store.iter_end_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("child store iter end", K(ret), K(i));
        } else {
          ObDASIter *child_iter = child_iters_.at(i);
          if (OB_ISNULL(child_iter)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", K(i));
          } else if (OB_FAIL(child_iter->get_next_row())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              child_store.iter_end_ = true;
              child_empty_count_ ++;
              iter_end_count_ ++;
            } else {
              LOG_WARN("failed to get next row from child iter", K(ret));
            }
          } else if (OB_FAIL(child_store.save(false, 1))) {
            LOG_WARN("failed to save child row", K(ret));
          } else if (child_iter->get_type() == DAS_ITER_SORT) {
            reset_datum_ptr(child_iter->get_output(), 1);
          }
        }
      }

      if (OB_FAIL(ret) || child_store.iter_end_) {
      } else if (last_child_store_idx == OB_INVALID_INDEX) {
        last_child_store_idx = i;
      } else if (last_child_store_idx == i) {
        // skip
      } else if (child_stores_.at(last_child_store_idx).is_empty()) {
        cmp_ret = -1;
        last_child_store_idx = i;
      } else {
        IndexMergeRowStore &last_child_store = child_stores_.at(last_child_store_idx);
        if (OB_FAIL(compare(child_store, last_child_store, cmp_ret))) {
          LOG_WARN("failed to compare row", K(ret), K(child_store), K(last_child_store));
        } else if ((cmp_ret == 0) && (i == child_cnt - 1)) {
          got_row = true;
        } else if (cmp_ret < 0) {
          child_store.incre_idx();
          last_child_store_idx = i;
        } else if (cmp_ret > 0) {
          last_child_store.incre_idx();
        }
      }
    } // end for
  } // end while

  if (OB_SUCC(ret) && got_row) {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
      IndexMergeRowStore &output_store = child_stores_.at(i);
      if (output_store.is_empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty store", K(ret), K(output_store));
      } else if (OB_FAIL(output_store.to_expr(1))) {
        LOG_WARN("failed to expr", K(ret), K(output_store));
      }
    }
  }

  return ret;
}

int ObDASIndexMergeAndIter::sort_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t child_cnt = child_stores_.count();
  int64_t last_child_store_idx = OB_INVALID_INDEX;
  if (rowkey_is_uint64_ && !is_reverse_) {
    if (child_cnt == 1) {
      IndexMergeRowStore &child_store = child_stores_.at(0);
      int64_t output_row_cnt = std::min(child_store.count() - child_store.idx_, capacity - count);
      if (OB_FAIL(child_store.to_expr(output_row_cnt))) {
        LOG_WARN("failed to expr", K(ret), K(child_store), K(output_row_cnt));
      } else if (OB_FAIL(save_row_to_result_buffer(output_row_cnt))) {
        LOG_WARN("failed to save row to result buffer", K(ret), K(child_store), K(output_row_cnt));
      } else {
        count += output_row_cnt;
      }
    } else {
      IndexMergeRowIdCursor *base_cursor = child_cursors_.at(0);
      if (OB_ISNULL(base_cursor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr cursor", K(ret));
      }
      uint64_t doc_id = 0;
      uint64_t next_id = 0;
      while (OB_SUCC(ret) && count < capacity) {
        if (base_cursor->is_exhausted()) {
          ret = OB_ITER_END;
        } else {
          doc_id = base_cursor->current_rowid();
        }
        while (OB_SUCC(ret)) {
          bool all_matched = true;
          for (int64_t i = 1; OB_SUCC(ret) && i < child_cnt; i++) {
            IndexMergeRowIdCursor *child_cursor = child_cursors_.at(i);
            if (OB_ISNULL(child_cursor)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected nullptr cursor", K(ret), K(i));
            } else if (OB_FAIL(child_cursor->advance_to(doc_id, next_id))) {
            } else if (doc_id != next_id) {
              doc_id = next_id;
              all_matched = false;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (all_matched) {
            break;
          } else if (OB_FAIL(base_cursor->advance_to(doc_id, next_id))) {
          } else if (doc_id != next_id) {
            doc_id = next_id;
          }
        }
        current_skip_id_ = std::max(current_skip_id_, doc_id);
        if (OB_SUCC(ret)) {
          // find a valid doc_id
          LOG_TRACE("find a valid doc_id", K(doc_id));
          for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
            IndexMergeRowIdCursor *output_cursor = child_cursors_.at(i);
            if (OB_ISNULL(output_cursor)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected nullptr cursor", K(ret), K(i));
            } else if (OB_FAIL(output_cursor->to_expr())) {
              LOG_WARN("failed to expr", K(ret), KPC(output_cursor));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(save_row_to_result_buffer(1))) {
            LOG_WARN("failed to save row to result buffer", K(ret));
          } else {
            count++;
          }
        }
      } // end while
    }
  } else {
    while (OB_SUCC(ret) && count < capacity) {
      int cmp_ret = 0;
      bool matched = false;
      for (int64_t i = 0; OB_SUCC(ret) && (cmp_ret == 0) && i < child_cnt; i++) {
        IndexMergeRowStore &child_store = child_stores_.at(i);
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
          if (OB_FAIL(compare(child_store, last_child_store, cmp_ret))) {
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
        for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
          IndexMergeRowStore &output_store = child_stores_.at(i);
          if (output_store.is_empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected empty store", K(ret), K(output_store));
          } else if (OB_FAIL(output_store.to_expr(1))) {
            LOG_WARN("failed to expr", K(ret), K(output_store));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(save_row_to_result_buffer(1))) {
          LOG_WARN("failed to save row to result buffer", K(ret));
        } else {
          count ++;
          last_child_store_idx = OB_INVALID_INDEX;
        }
      }
    } // end while
  }

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDASIndexMergeAndIter::check_can_be_shorted()
{
  int ret = OB_SUCCESS;
  can_be_shorted_ = true;
  int64_t child_cnt = child_iters_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
    ObDASIter *child = child_iters_.at(i);
    if (OB_ISNULL(child) || OB_ISNULL(child->get_output())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid child iter", K(i), K(child), K(ret));
    } else {
      const common::ObIArray<ObExpr*> &exprs = *child->get_output();
      for (int64_t j = 0; OB_SUCC(ret) && can_be_shorted_ && j < exprs.count(); j++) {
        ObExpr *expr = exprs.at(j);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr expr", K(ret));
        } else if (T_FUN_MATCH_AGAINST == expr->type_) {
          can_be_shorted_ = false;
        }
      }
    }
  }
  return ret;
}

int ObDASIndexMergeAndIter::SortedRowIdCursor::init(ObDASIndexMergeIter::IndexMergeRowStore *store)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(store));
  } else {
    store_ = store;
  }
  return ret;
}

int ObDASIndexMergeAndIter::BitmapRowIdCursor::init(
    common::ObIAllocator &allocator,
    ObDASScanIter *scan_iter,
    int64_t scan_size,
    ObExpr *rowid_expr,
    ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_iter) || OB_ISNULL(rowid_expr) || OB_ISNULL(eval_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for bitmap cursor init", K(ret), KP(scan_iter), KP(rowid_expr), KP(eval_ctx));
  } else if (OB_UNLIKELY(scan_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid scan_size for bitmap cursor", K(ret), K(scan_size));
  } else if (OB_ISNULL(bitmap_ = OB_NEWx(ObRoaringBitmap, &allocator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate roaring bitmap", K(ret));
  } else {
    allocator_ = &allocator;
    scan_iter_ = scan_iter;
    scan_size_ = scan_size;
    rowid_expr_ = rowid_expr;
    eval_ctx_ = eval_ctx;
    bitmap_->set_empty();
    exhausted_ = true;
    bitmap_iter_ = nullptr;
    LOG_TRACE("bitmap cursor init", K(scan_size));
  }
  return ret;
}

int ObDASIndexMergeAndIter::BitmapRowIdCursor::build_bitmap()
{
  int ret = OB_SUCCESS;

  int64_t total_count = 0;
  while (OB_SUCC(ret)) {
    int64_t count = 0;
    if (OB_FAIL(scan_iter_->get_next_rows(count, scan_size_))) {
      if (OB_UNLIKELY(ret != OB_ITER_END)) {
        LOG_WARN("failed to get next rows from scan", K(ret), K_(scan_size));
      } else {
        if (count > 0) {
          ret = OB_SUCCESS;
        }
      }
    }
    if (OB_SUCC(ret) && count > 0) {
      total_count += count;
      ObDatum *datums = rowid_expr_->locate_batch_datums(*eval_ctx_);
      if (OB_ISNULL(datums)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to locate batch datums", K(ret), K(count));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          uint64_t rowid = datums[i].get_uint64();
          if (OB_FAIL(bitmap_->value_add(rowid))) {
            LOG_WARN("failed to add rowid to bitmap", K(ret), K(rowid), K(i));
          }
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      break;
    }
  }

  if (OB_SUCC(ret)) {
    uint64_t cardinality = bitmap_->get_cardinality();
    if (cardinality == 0) {
      exhausted_ = true;
      LOG_TRACE("bitmap built with no data", K(total_count));
    } else {
      if (OB_ISNULL(bitmap_iter_ = static_cast<ObRoaringBitmapIter *>(allocator_->alloc(sizeof(ObRoaringBitmapIter))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate bitmap iterator", K(ret));
      } else {
        new (bitmap_iter_) ObRoaringBitmapIter(bitmap_);
        if (OB_FAIL(bitmap_iter_->init())) {
          LOG_WARN("failed to init bitmap iterator", K(ret), K(cardinality));
        } else {
          exhausted_ = false;
        }
      }
    }
    LOG_TRACE("bitmap build complete", K(ret), K(total_count), K(cardinality), K_(exhausted));
  }

  return ret;
}

int ObDASIndexMergeAndIter::BitmapRowIdCursor::advance_to(uint64_t target, uint64_t &rowid)
{
  int ret = OB_SUCCESS;
  if (exhausted_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(bitmap_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("bitmap iterator not initialized", K(ret));
  } else {
    uint64_t curr_val = bitmap_iter_->get_curr_value();
    // advance until we find a value >= target
    while (OB_SUCC(ret) && curr_val < target) {
      if (OB_FAIL(bitmap_iter_->get_next())) {
        if (ret == OB_ITER_END) {
          exhausted_ = true;
        } else {
          LOG_WARN("failed to get next from bitmap", K(ret), K(target), K(curr_val));
        }
      } else {
        curr_val = bitmap_iter_->get_curr_value();
      }
    }
    if (OB_SUCC(ret)) {
      rowid = curr_val;
    }
  }
  return ret;
}

int ObDASIndexMergeAndIter::BitmapRowIdCursor::to_expr()
{
  int ret = OB_SUCCESS;
  if (exhausted_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap cursor exhausted, cannot output", K(ret));
  } else if (OB_ISNULL(bitmap_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("bitmap iterator not initialized", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    batch_info_guard.set_batch_size(1);
    batch_info_guard.set_batch_idx(0);
    uint64_t curr_val = bitmap_iter_->get_curr_value();
    ObDatum &datum = rowid_expr_->locate_datum_for_write(*eval_ctx_);
    datum.set_uint(curr_val);
    rowid_expr_->set_evaluated_projected(*eval_ctx_);

    // advance to next position
    if (OB_FAIL(bitmap_iter_->get_next())) {
      if (ret == OB_ITER_END) {
        exhausted_ = true;
        ret = OB_SUCCESS;  // to_expr succeeded, just no more data
      } else {
        LOG_WARN("failed to advance bitmap iterator", K(ret), K(curr_val));
      }
    }

    LOG_TRACE("bitmap cursor output rowid", K(ret), K(curr_val), K_(exhausted));
  }
  return ret;
}

void ObDASIndexMergeAndIter::BitmapRowIdCursor::reuse()
{
  if (OB_NOT_NULL(bitmap_iter_)) {
    bitmap_iter_->deinit();
    bitmap_iter_ = nullptr;
  }
  if (OB_NOT_NULL(bitmap_)) {
    bitmap_->set_empty();
  }
  exhausted_ = true;
  counted_as_empty_ = false;
}

void ObDASIndexMergeAndIter::BitmapRowIdCursor::reset()
{
  if (OB_NOT_NULL(bitmap_iter_)) {
    bitmap_iter_->deinit();
    bitmap_iter_ = nullptr;
  }
  if (OB_NOT_NULL(bitmap_)) {
    bitmap_->set_empty();
    bitmap_ = nullptr;
  }
  exhausted_ = true;
  allocator_ = nullptr;
  scan_iter_ = nullptr;
  scan_size_ = 0;
  rowid_expr_ = nullptr;
  eval_ctx_ = nullptr;
}

bool ObDASIndexMergeAndIter::should_use_bitmap_for_child(int64_t child_idx) const
{
  bool use_bitmap = false;
  if (child_idx < 0 || child_idx >= merge_ctdef_->children_cnt_ || merge_ctdef_->children_cnt_ == 1) {
    use_bitmap = false;
  } else {
    const ObDASBaseCtDef *child_ctdef = merge_ctdef_->children_[child_idx];
    use_bitmap = OB_NOT_NULL(child_ctdef) &&
                 child_ctdef->op_type_ == DAS_OP_SORT &&  // non-ROR index has SORT operator inserted by optimizer
                 merge_ctdef_->merge_node_types_.at(child_idx) == INDEX_MERGE_SCAN;
  }
  return use_bitmap;
}

ObDASScanIter* ObDASIndexMergeAndIter::extract_scan_iter_from_child(int64_t child_idx) const
{
  ObDASIter *child_iter = child_iters_.at(child_idx);
  ObDASScanIter *scan_iter = nullptr;
  if (OB_ISNULL(child_iter)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected null child iter", K(child_idx));
  } else if (child_iter->get_children_cnt() != 1 || OB_ISNULL(child_iter->get_children())) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "expect exactly one child (scan iter) under sort iter", K(child_idx), K(child_iter->get_children_cnt()));
  } else if (OB_ISNULL(child_iter->get_children()[0])) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected null scan iter under sort iter", K(child_idx));
  } else {
    scan_iter = static_cast<ObDASScanIter*>(child_iter->get_children()[0]);
  }
  return scan_iter;
}

int ObDASIndexMergeAndIter::init_child_cursors()
{
  int ret = OB_SUCCESS;
  int64_t child_cnt = child_stores_.count();
  common::ObArenaAllocator &allocator = mem_ctx_->get_arena_allocator();

  if (FALSE_IT(child_cursors_.set_allocator(&allocator))) {
  } else if (OB_FAIL(child_cursors_.prepare_allocate(child_cnt))) {
    LOG_WARN("failed to prepare allocate child cursors", K(ret), K(child_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
      IndexMergeRowIdCursor *cursor;
      if (enable_bitmap_cursor_ && should_use_bitmap_for_child(i)) {
        BitmapRowIdCursor *bitmap_cursor = OB_NEWx(BitmapRowIdCursor, &allocator);
        ObDASScanIter *scan_iter = nullptr;
        if (OB_ISNULL(bitmap_cursor)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate bitmap cursor", K(ret), K(i));
        } else if (OB_ISNULL(scan_iter = extract_scan_iter_from_child(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to extract scan iter from child", K(ret), K(i));
        } else if (OB_FAIL(bitmap_cursor->init(allocator,
                                               scan_iter,
                                               child_stores_.at(i).capacity_,
                                               rowkey_exprs_->at(0),
                                               eval_ctx_))) {
          LOG_WARN("failed to init bitmap cursor", K(ret), K(i));
        } else {
          cursor = bitmap_cursor;
          LOG_TRACE("created bitmap cursor for child", K(i));
        }
      } else {
        SortedRowIdCursor *sorted_cursor = OB_NEWx(SortedRowIdCursor, &allocator);
        if (OB_ISNULL(sorted_cursor)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate sorted cursor", K(ret), K(i));
        } else if (OB_FAIL(sorted_cursor->init(&child_stores_.at(i)))) {
          LOG_WARN("failed to init sorted cursor", K(ret), K(i));
        } else {
          cursor = sorted_cursor;
          LOG_TRACE("created sorted cursor for child", K(i));
        }
      }
      if (OB_SUCC(ret)) {
        child_cursors_.at(i) = cursor;
      }
    }
  }
  LOG_TRACE("init child cursors", K(ret), K(child_cnt), K(child_cursors_.count()));
  return ret;
}

int ObDASIndexMergeAndIter::check_child_cursors()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && child_empty_count_ <= 0 && i < child_cursors_.count(); ++i) {
    if (!should_use_bitmap_for_child(i)) {
    } else {
      BitmapRowIdCursor *bitmap_cursor = static_cast<BitmapRowIdCursor *>(child_cursors_.at(i));
      if (OB_ISNULL(bitmap_cursor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr bitmap cursor", K(ret), K(i));
      } else if (bitmap_cursor->counted_as_empty_) {
      } else if (bitmap_cursor->is_exhausted()) {
        bitmap_cursor->counted_as_empty_ = true;
        ++child_empty_count_;
        LOG_TRACE("bitmap cursor exhausted", K(i), K_(child_empty_count));
      }
    }
  }
  return ret;
}

int ObDASIndexMergeAndIter::build_bitmaps_for_children()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_cursors_.count(); ++i) {
    if (should_use_bitmap_for_child(i)) {
      BitmapRowIdCursor *bitmap_cursor = static_cast<BitmapRowIdCursor *>(child_cursors_.at(i));
      if (OB_ISNULL(bitmap_cursor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr bitmap cursor", K(ret), K(i));
      } else if (OB_FAIL(bitmap_cursor->build_bitmap())) {
        LOG_WARN("failed to build bitmap for cursor", K(ret), K(i));
      } else {
        LOG_TRACE("bitmap cursor built successfully", K(i));
        child_stores_.at(i).iter_end_ = true;
        child_stores_.at(i).drained_ = true;
      }
    }
  }
  return ret;
}

int ObDASIndexMergeAndIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASIndexMergeIter::do_table_scan())) {
    LOG_WARN("failed to do table scan in parent", K(ret));
  } else if (enable_bitmap_cursor_ && OB_FAIL(build_bitmaps_for_children())) {
    LOG_WARN("failed to build bitmaps for children", K(ret));
  }
  return ret;
}

int ObDASIndexMergeAndIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASIndexMergeIter::rescan())) {
    LOG_WARN("failed to rescan in parent", K(ret));
  } else if (enable_bitmap_cursor_ && OB_FAIL(build_bitmaps_for_children())) {
    LOG_WARN("failed to build bitmaps for children in rescan", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
