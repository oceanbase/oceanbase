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
#include "sql/das/iter/ob_das_index_merge_or_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASIndexMergeOrIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (ObDASIterType::DAS_ITER_INDEX_MERGE != param.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param));
  } else {
    ObDASIndexMergeIterParam &index_merge_param = static_cast<ObDASIndexMergeIterParam&>(param);
    if (index_merge_param.merge_type_ != INDEX_MERGE_UNION) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index merge iter param with bad merge type", K(index_merge_param.merge_type_));
    } else if (OB_FAIL(ObDASIndexMergeIter::inner_init(param))) {
      LOG_WARN("inner init index merge or iter failed", K(param));
    } else {
      int64_t child_cnt = index_merge_param.child_iters_->count();
      common::ObArenaAllocator &alloc = mem_ctx_->get_arena_allocator();
      child_match_against_exprs_.set_allocator(&alloc);
      if (OB_FAIL(child_match_against_exprs_.prepare_allocate(child_cnt))) {
        LOG_WARN("failed to prepare allocate child match against exprs", K(ret));
      } else {
        ObArray<ObExpr*> match_against_exprs;
        for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
          ObDASIter *child = child_iters_.at(i);
          ExprFixedArray &child_match_against_expr = child_match_against_exprs_.at(i);
          if (OB_ISNULL(child) || OB_ISNULL(child->get_output())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid child iter", K(i), K(child), K(ret));
          } else if (OB_FAIL(extract_match_against_exprs(*child->get_output(), match_against_exprs))) {
            LOG_WARN("failed to extract match against exprs", K(ret));
          } else if (FALSE_IT(child_match_against_expr.set_allocator(&alloc))) {
          } else if (OB_FAIL(child_match_against_expr.assign(match_against_exprs))) {
            LOG_WARN("failed to assign match against exprs", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASIndexMergeOrIter::inner_release()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < child_match_against_exprs_.count(); ++i) {
    child_match_against_exprs_.at(i).reset();
  }
  child_match_against_exprs_.reset();

  if (OB_FAIL(ObDASIndexMergeIter::inner_release())) {
    LOG_WARN("inner release index merge or iter failed", K(ret));
  }

  return ret;
}

int ObDASIndexMergeOrIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(sort_get_next_row())) {
    LOG_WARN("index merge iter failed to get next row", K(ret));
  } else if (child_empty_count_ >= child_stores_.count()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObDASIndexMergeOrIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t child_cnt = child_stores_.count();
  count = 0;
  while (OB_SUCC(ret) && child_empty_count_ < child_cnt && count < capacity) {
    clear_evaluated_flag();
    if (OB_FAIL(fill_child_stores(capacity))) {
      LOG_WARN("fill child stores failed", K(ret), K(child_empty_count_));
    } else if (child_empty_count_ >= child_cnt) {
      ret = OB_ITER_END;
    } else {
      bool use_bitmap = false;
      bool is_intersected = true;
      uint64_t range_dense = 0;
      if (!rowkey_is_uint64_) {
        // do nothing, just get next rows by sort
      } else if (OB_UNLIKELY(force_merge_mode_ != 0)) {
        use_bitmap = disable_bitmap_ ? false : force_merge_mode_ == 2;
      } else {
        if (OB_FAIL(rowkey_range_is_all_intersected(is_intersected))) {
          LOG_WARN("index merge iter failed to check rowkey range is intersected", K(ret));
        } else if (!is_intersected) {
          use_bitmap = disable_bitmap_ ? false : true;
        } else if (!disable_bitmap_) {
          if (OB_FAIL(rowkey_range_dense(range_dense))) {
            LOG_WARN("index merge iter failed to get rowkey range dense", K(ret));
          } else if (range_dense > 10) {
            use_bitmap = true;
          }
        }
      }

      LOG_TRACE("select the merge mode", K(use_bitmap), K(disable_bitmap_), K(is_intersected), K(range_dense), K(force_merge_mode_));
      if (OB_FAIL(ret)) {
      } else if (!use_bitmap) {
        if (OB_FAIL(sort_get_next_rows(count, capacity))){
          LOG_WARN("index merge iter failed to get next row by sort", K(ret), K(count), K(capacity));
        } else {
          LOG_TRACE("[DAS ITER] index merge iter get next rows by sort", K(count), K(capacity), K(ret));
        }
      } else {
        if (OB_FAIL(bitmap_get_next_rows(count, capacity))){
          LOG_WARN("index merge iter failed to get next row by bitmap", K(ret), K(count), K(capacity));
        } else {
          LOG_TRACE("[DAS ITER] index merge iter get next rows by bitmap", K(count), K(capacity), K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_LIKELY(ret == OB_ITER_END)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(result_buffer_rows_to_expr(count))) {
        ret = tmp_ret;
        LOG_WARN("output rows to expr failed", K(ret), K(count), K(capacity));
      }
    }
  } else if (count == 0 && child_empty_count_ >= child_cnt) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(result_buffer_rows_to_expr(count))) {
    LOG_WARN("output all row to expr failed", K(ret), K(count), K(capacity));
  }

  const ObBitVector *skip = nullptr;
  PRINT_VECTORIZED_ROWS(SQL, DEBUG, *eval_ctx_, *output_, count, skip);

  return ret;
}

int ObDASIndexMergeOrIter::extract_match_against_exprs(const common::ObIArray<ObExpr*> &exprs,
                                                       common::ObIArray<ObExpr*> &match_against_exprs) const
{
  int ret = OB_SUCCESS;
  match_against_exprs.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObExpr *expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr expr", K(ret));
    } else if (T_FUN_MATCH_AGAINST == expr->type_) {
      if (OB_FAIL(match_against_exprs.push_back(expr))) {
        LOG_WARN("failed to push back match against expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDASIndexMergeOrIter::fill_default_values(const common::ObIArray<ObExpr*> &exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObExpr *expr = exprs.at(i);
    if (OB_ISNULL(expr) || OB_UNLIKELY(T_FUN_MATCH_AGAINST != expr->type_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid expr to fill default value", KP(expr), K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(1);
      batch_info_guard.set_batch_idx(0);
      ObDatum &datum = expr->locate_datum_for_write(*eval_ctx_);
      datum.set_double(0.0);
      expr->set_evaluated_flag(*eval_ctx_);
      expr->set_evaluated_projected(*eval_ctx_);
    }
  }
  return ret;
}

int ObDASIndexMergeOrIter::check_direct(bool &can_derect, int64_t &output_idx) const
{
  int ret = OB_SUCCESS;
  can_derect = true;
  output_idx = OB_INVALID_INDEX;
  uint64_t cur_min_rowkey = 0;
  uint64_t cur_max_rowkey = UINT64_MAX;
  uint64_t min_rowkey = 0;
  uint64_t max_rowkey = UINT64_MAX;
  // find output_idx of the smallest or largest(if reverse) max row
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
    const IndexMergeRowStore &child_store = child_stores_.at(i);
    if (child_store.is_empty()){
    } else if (OB_FAIL(child_store.get_min_max_rowkey(min_rowkey, max_rowkey))) {
      LOG_WARN("index merge iter failed to get min max rowkey", K(ret), K(i));
    } else if (output_idx == OB_INVALID_INDEX) {
      output_idx = i;
      cur_min_rowkey = min_rowkey;
      cur_max_rowkey = max_rowkey;
    } else if (OB_LIKELY(!is_reverse_) && cur_max_rowkey > max_rowkey) {
      output_idx = i;
      cur_min_rowkey = min_rowkey;
      cur_max_rowkey = max_rowkey;
    } else if (OB_UNLIKELY(is_reverse_) && cur_max_rowkey < max_rowkey) {
      output_idx = i;
      cur_min_rowkey = min_rowkey;
      cur_max_rowkey = max_rowkey;
    }
  }

  if (OB_SUCC(ret) && OB_LIKELY(output_idx != OB_INVALID_INDEX)) {
    // check output_idx does not intersect other iter
    for (int64_t i = 0; OB_SUCC(ret) && !can_derect && i < child_stores_.count(); i++) {
      const IndexMergeRowStore &child_store = child_stores_.at(i);
      if (output_idx == i) {
        // skip
      } else if (OB_FAIL(child_store.get_min_max_rowkey(min_rowkey, max_rowkey))) {
        LOG_WARN("index merge iter failed to get min max rowkey", K(ret), K(i));
      } else if (min_rowkey <= cur_max_rowkey && max_rowkey >= cur_min_rowkey) {
        can_derect = false;
      }
    }
  } else {
    can_derect = false;
  }
	return ret;
}

int ObDASIndexMergeOrIter::sort_get_next_row()
{
  int ret = OB_SUCCESS;
  /* try to fill each child store */
  int cmp_ret = 0;
  int64_t child_cnt = child_stores_.count();
  int64_t last_child_store_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
    IndexMergeRowStore &child_store = child_stores_.at(i);
    cmp_ret = 0;
    if (child_store.is_empty() && !child_store.iter_end_) {
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
      }
    }
    if (OB_FAIL(ret) || child_store.iter_end_) {
    } else if (last_child_store_idx == OB_INVALID_INDEX) {
      last_child_store_idx = i;
    } else if (OB_UNLIKELY(child_stores_.at(last_child_store_idx).is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected last child store idx", K(i), K(last_child_store_idx));
    } else {
      IndexMergeRowStore &last_child_store = child_stores_.at(last_child_store_idx);
      if (OB_FAIL(compare(child_store, last_child_store, cmp_ret))) {
        LOG_WARN("failed to compare row", K(ret), K(child_store), K(last_child_store));
      } else if (cmp_ret < 0) {
        last_child_store_idx = i;
      }
    }
  } // end for

  // TODO: use min-heap optimization
  if (OB_FAIL(ret)) {
  } else if (last_child_store_idx == OB_INVALID_INDEX) {
    /* no available row, stop the entire process */
    if (OB_UNLIKELY(child_empty_count_ != child_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected iter end count", K(child_empty_count_), K(child_cnt));
    }
  } else if (OB_UNLIKELY(child_stores_.at(last_child_store_idx).is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected last child store idx", K(ret), K(last_child_store_idx));
  } else {
    /* find all available rows */
    bool need_fill_default_value = false;
    IndexMergeRowStore &last_child_store = child_stores_.at(last_child_store_idx);
    for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
      IndexMergeRowStore &child_store = child_stores_.at(i);
      need_fill_default_value = false;
      if (last_child_store_idx == i) {
        /* skip */
      } else if (!child_store.is_empty()) {
        if (OB_FAIL(compare(child_store, last_child_store, cmp_ret))) {
          LOG_WARN("index merge failed to compare row", K(i), K(last_child_store_idx), K(ret));
        } else if (cmp_ret == 0) {
          if (OB_FAIL(child_store.to_expr(false, 1))) {
            LOG_WARN("failed to convert store row to expr", K(ret));
          }
        } else {
          // this child does not have a corresponding rowkey
          need_fill_default_value = true;
        }
      } else {
        // this child does not have available rows
        need_fill_default_value = true;
      }
      if (OB_SUCC(ret) && need_fill_default_value) {
        if (OB_FAIL(fill_default_values(child_match_against_exprs_.at(i)))) {
          LOG_WARN("failed to fill default values for union", K(ret), K(i));
        }
      }
    } // end for
    if (OB_SUCC(ret)) {
      if (OB_FAIL(child_stores_.at(last_child_store_idx).to_expr(false, 1))) {
        LOG_WARN("failed to convert store row to expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDASIndexMergeOrIter::sort_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t child_cnt = child_stores_.count();
  while (OB_SUCC(ret) && count < capacity) {
    /* try to fill each child store */
    int cmp_ret = 0;
    int64_t last_child_store_idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
      IndexMergeRowStore &child_store = child_stores_.at(i);
      cmp_ret = 0;
      if (child_store.is_empty()) {
        if (child_store.iter_end_) {
          // skip
        } else {
          // avoid outputting duplicate values
          ret = OB_ITER_END;
        }
      } else if (last_child_store_idx == OB_INVALID_INDEX) {
        last_child_store_idx = i;
      } else if (OB_UNLIKELY(child_stores_.at(last_child_store_idx).is_empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected last child store idx", K(i), K(last_child_store_idx));
      } else {
        IndexMergeRowStore &last_child_store = child_stores_.at(last_child_store_idx);
        if (OB_FAIL(compare(child_store, last_child_store, cmp_ret))) {
          LOG_WARN("failed to compare row", K(ret), K(child_store), K(last_child_store));
        } else if (cmp_ret < 0) {
          last_child_store_idx = i;
        }
      }
    } // end for

    if (OB_FAIL(ret)) {
    } else if (last_child_store_idx == OB_INVALID_INDEX) {
      /* no available row, stop the entire process */
      ret = OB_ITER_END;
    } else if (OB_UNLIKELY(child_stores_.at(last_child_store_idx).is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected last child store idx", K(ret), K(last_child_store_idx));
    } else {
      /* find all available rows */
      bool need_fill_default_value = false;
      IndexMergeRowStore &last_child_store = child_stores_.at(last_child_store_idx);
      for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
        IndexMergeRowStore &child_store = child_stores_.at(i);
        need_fill_default_value = false;
        if (last_child_store_idx == i) {
          /* skip */
        } else if (!child_store.is_empty()) {
          if (OB_FAIL(compare(child_store, last_child_store, cmp_ret))) {
            LOG_WARN("index merge failed to compare row", K(ret), K(i), K(last_child_store_idx));
          } else if (cmp_ret == 0) {
            if (OB_FAIL(child_store.to_expr(true, 1))) {
              LOG_WARN("failed to convert store row to expr", K(ret));
            }
          } else {
            // this child does not have a corresponding rowkey
            need_fill_default_value = true;
          }
        } else {
          // this child does not have available rows
          need_fill_default_value = true;
        }
        if (OB_SUCC(ret) && need_fill_default_value) {
          if (OB_FAIL(fill_default_values(child_match_against_exprs_.at(i)))) {
            LOG_WARN("failed to fill default values for union", K(ret), K(i));
          }
        }
      } // end for

      if (OB_SUCC(ret)) {
        if (OB_FAIL(child_stores_.at(last_child_store_idx).to_expr(true, 1))) {
          LOG_WARN("failed to convert store row to expr", K(ret));
        } else {
          // now we get a available result row, save it to result buffer
          if (OB_FAIL(save_row_to_result_buffer(1))) {
            LOG_WARN("failed to save row to result buffer", K(ret));
          } else {
            count ++;
          }
        }
      }
    }
  } // end while

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDASIndexMergeOrIter::bitmap_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObDASIndexMergeOrIter::bitmap_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t child_cnt = child_stores_.count();
  if (OB_UNLIKELY(disable_bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is disabled", K(ret), K(disable_bitmap_));
  } else if (OB_UNLIKELY(!rowkey_is_uint64_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey is not uint64", K(ret), K(rowkey_is_uint64_));
  } else if (OB_UNLIKELY(child_empty_count_ >= child_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child empty count", K(ret), K(child_empty_count_), K(child_cnt));
  } else if (OB_FAIL(fill_child_bitmaps())) {
    LOG_WARN("failed to fill child bitmaps", K(ret));
  } else if (OB_ISNULL(result_bitmap_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(result_bitmap_iter_));
  } else if (OB_UNLIKELY(child_bitmaps_.count() != child_cnt ||
                          child_bitmaps_.count() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child bitmap count", K(ret), K(child_cnt), K(child_bitmaps_.count()));
  } else if (OB_ISNULL(child_bitmaps_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(child_bitmaps_.at(0)));
  } else {
    bool got_first_row = false;
    uint64_t rowkey = 0;
    ObRoaringBitmap *result_bitmap = child_bitmaps_.at(0);
    for (int64_t i = 1; OB_SUCC(ret) && i < child_cnt; i++) {
      ObRoaringBitmap *child_bitmap = child_bitmaps_.at(i);
      if (OB_ISNULL(child_bitmap)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(child_bitmap));
      } else if (OB_FAIL(result_bitmap->value_or(child_bitmap))) {
        LOG_WARN("failed to init result bitmap", K(ret), K(child_bitmaps_.count()), KPC(result_bitmap));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(result_bitmap_iter_->init(is_reverse_))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to init bitmap iter", K(ret), K(result_bitmap_iter_));
      }
    }
    while (OB_SUCC(ret) && count < capacity) {
      if (!got_first_row) {
        got_first_row = true;
        rowkey = result_bitmap_iter_->get_curr_value();
      } else if (OB_FAIL(result_bitmap_iter_->get_next())) {
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          LOG_WARN("failed to get next from bitmap iter", K(ret));
        }
      } else {
        rowkey = result_bitmap_iter_->get_curr_value();
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
        bool finded = false;
        IndexMergeRowStore &child_store = child_stores_.at(i);
        if (OB_FAIL(child_store.locate_rowkey(rowkey, finded))) {
          LOG_WARN("failed to locate rowkey", K(ret), K(i), K(rowkey), K(child_store));
        } else if (!finded && OB_FAIL(fill_default_values(child_match_against_exprs_.at(i)))) {
          LOG_WARN("failed to fill default values for union", K(ret), K(i));
        } else if (finded && OB_FAIL(child_store.to_expr(true, 1))) {
          LOG_WARN("failed to expr", K(ret), K(i));
        }
      } // end for

      if (OB_SUCC(ret)) {
        if (OB_FAIL(save_row_to_result_buffer(1))) {
          LOG_WARN("failed to save row to result buffer", K(ret), K(child_cnt));
        } else {
          count ++;
        }
      }
    } // end while

    if (OB_LIKELY(ret == OB_ITER_END)) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      result_bitmap_iter_->deinit();
    }
  }
  return ret;
}

int ObDASIndexMergeOrIter::direct_get_next_row(int64_t output_idx)
{
  return OB_NOT_IMPLEMENT;
}

int ObDASIndexMergeOrIter::direct_get_next_rows(int64_t &count, int64_t capacity, int64_t output_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(output_idx < 0 ||
      output_idx >= child_stores_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no available child", K(ret), K(output_idx));
  } else {
    IndexMergeRowStore &output_store = child_stores_.at(output_idx);
    while(OB_SUCC(ret) && !output_store.is_empty() && count < capacity) {
      if (OB_FAIL(output_store.to_expr(true, 1))) {
        LOG_WARN("failed to convert store row to expr", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
          if (i != output_idx && OB_FAIL(fill_default_values(child_match_against_exprs_.at(i)))) {
            LOG_WARN("failed to fill default values for union", K(ret), K(i));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(save_row_to_result_buffer(1))) {
          LOG_WARN("failed to save row to result buffer", K(ret));
        } else {
          count ++;
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
