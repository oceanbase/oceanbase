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

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASIndexMergeAndIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (ObDASIterType::DAS_ITER_INDEX_MERGE != param.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param));
  } else {
    ObDASIndexMergeIterParam &index_merge_param = static_cast<ObDASIndexMergeIterParam&>(param);
    if (index_merge_param.merge_type_ != INDEX_MERGE_INTERSECT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index merge iter param with bad merge type", K(index_merge_param.merge_type_));
    } else if (OB_FAIL(ObDASIndexMergeIter::inner_init(param))) {
      LOG_WARN("inner init index merge and iter failed", K(param));
    } else if (OB_FAIL(check_can_be_shorted())) {
      LOG_WARN("index merge iter failed to check can be shorted", K(ret));
    } else {
      shorted_child_idx_ = OB_INVALID_INDEX;
    }
  }
  return ret;
}

int ObDASIndexMergeAndIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASIndexMergeIter::inner_reuse())) {
    LOG_WARN("index merge iter failed to reuse", K(ret));
  } else {
    shorted_child_idx_ = OB_INVALID_INDEX;
  }
  return ret;
}

int ObDASIndexMergeAndIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASIndexMergeIter::inner_release())) {
    LOG_WARN("inner release index merge or iter failed");
  } else {
    shorted_child_idx_ = OB_INVALID_INDEX;
  }
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
  count = 0;
  while (OB_SUCC(ret) && child_empty_count_ <= 0 && count < capacity) {
    clear_evaluated_flag();
    if (OB_FAIL(fill_child_stores(capacity))) {
      LOG_WARN("fill child stores failed", K(ret), K(child_empty_count_));
    } else if (child_empty_count_ > 0) {
      ret = OB_ITER_END;
    } else if (can_be_shorted_ && iter_end_count_ > 0 && force_merge_mode_ == 0) {
      // it ends early and do rescan directly when a child has iter end
      LOG_TRACE("shorted path", K(can_be_shorted_), K(iter_end_count_), K(child_empty_count_), K(count), K(capacity));
      if (OB_FAIL(shorted_get_next_rows(count, capacity))) {
        LOG_WARN("index merge iter failed to get next row by shorted", K(ret), K(count), K(capacity));
      }
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
          // there is no intersection, optimistically, the branch with the smallest rowkey can be simply discarded
          int64_t idx = OB_INVALID_INDEX;
          uint64_t cur_rowkey = OB_LIKELY(!is_reverse_) ? UINT64_MAX : 0;
          uint64_t min_rowkey = 0;
          uint64_t max_rowkey = UINT64_MAX;
          for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
            IndexMergeRowStore &child_store = child_stores_.at(i);
            if (OB_FAIL(child_store.get_min_max_rowkey(min_rowkey, max_rowkey))) {
              LOG_WARN("index merge iter failed to get min max rowkey", K(ret), K(i));
            } else if (OB_LIKELY(!is_reverse_ && cur_rowkey > max_rowkey)) {
              idx = i;
              cur_rowkey = max_rowkey;
            } else if (OB_UNLIKELY(is_reverse_ && cur_rowkey < min_rowkey)) {
              idx = i;
              cur_rowkey = min_rowkey;
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(idx == OB_INVALID_INDEX)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get output child store failed", K(ret), K(idx), K(cur_rowkey), K(max_rowkey));
          } else {
            child_stores_.at(idx).reuse();
          }
        } else if (!disable_bitmap_) {
          if (OB_FAIL(rowkey_range_dense(range_dense))) {
            LOG_WARN("index merge iter failed to get rowkey range dense", K(ret));
          } else if (range_dense > 10) {
            use_bitmap = true;
          }
        }
      }

      LOG_TRACE("select the merge mode", K(use_bitmap), K(disable_bitmap_), K(is_intersected), K(range_dense), K(force_merge_mode_));
      if (OB_FAIL(ret) || !is_intersected) {
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
    } else if (OB_FAIL(child_store.to_expr(true, output_row_cnt))) {
      LOG_WARN("failed to expr", K(ret), K(shorted_child_idx_), K(child_store), K(output_row_cnt), K(capacity));
    } else if (OB_FAIL(save_row_to_result_buffer(output_row_cnt))) {
      LOG_WARN("failed to save row to result buffer", K(ret), K(shorted_child_idx_), K(output_row_cnt));
    } else {
      count += output_row_cnt;
    }
  }
  return ret;
}

int ObDASIndexMergeAndIter::bitmap_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObDASIndexMergeAndIter::bitmap_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t child_cnt = child_stores_.count();
  if (OB_UNLIKELY(disable_bitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is disabled", K(ret), K(disable_bitmap_));
  } else if (OB_UNLIKELY(!rowkey_is_uint64_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey is not uint64", K(ret), K(rowkey_is_uint64_));
  } else if (OB_UNLIKELY(child_empty_count_ > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter end count is larger than 0", K(ret), K(child_empty_count_));
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
    uint64_t last_valid_rowkey = 0;
    ObRoaringBitmap *result_bitmap = child_bitmaps_.at(0);
    // it is guaranteed that all the child bitmaps here will intersect after the AND operation
    for (int64_t i = 1; OB_SUCC(ret) && i < child_cnt; i++) {
      ObRoaringBitmap *child_bitmap = child_bitmaps_.at(i);
      if (OB_ISNULL(child_bitmap)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(child_bitmap));
      } else if (OB_FAIL(result_bitmap->value_and(child_bitmap))) {
        LOG_WARN("failed to init result bitmap", K(ret), K(child_bitmaps_.count()), KPC(result_bitmap));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_child_stores_last_valid_rowkey(last_valid_rowkey))) {
      LOG_WARN("failed to get child stores last valid rowkey", K(ret), KPC(result_bitmap), K(last_valid_rowkey));
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
        } else if (OB_UNLIKELY(!finded)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected rowkey", K(ret), K(i), K(child_store));
        } else if (OB_FAIL(child_store.to_expr(true, 1))) {
            LOG_WARN("failed to expr", K(ret), K(i));
        }
      } // end for

      if (OB_SUCC(ret)) {
        if (OB_FAIL(save_row_to_result_buffer(1))) {
          LOG_WARN("failed to save row to result buffer", K(ret), K(child_cnt));
        } else {
          count ++;
          last_valid_rowkey = rowkey;
        }
      }
    } // end while

    if (OB_LIKELY(ret == OB_ITER_END)) {
      ret = OB_SUCCESS;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(locate_child_stores_to_valid_rowkey(last_valid_rowkey))) {
      LOG_WARN("failed to locate child stores to valid rowkey", K(ret), K(last_valid_rowkey));
    } else {
      result_bitmap_iter_->deinit();
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
          child_store.head_next();
          last_child_store_idx = i;
        } else if (cmp_ret > 0) {
          last_child_store.head_next();
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
      } else if (OB_FAIL(output_store.to_expr(false, 1))) {
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
          child_store.head_next();
          last_child_store_idx = i;
        } else if (cmp_ret > 0) {
          last_child_store.head_next();
        }
      }
    } // end for

    if (OB_SUCC(ret) && cmp_ret == 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
        IndexMergeRowStore &output_store = child_stores_.at(i);
        if (output_store.is_empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected empty store", K(ret), K(output_store));
        } else if (OB_FAIL(output_store.to_expr(true, 1))) {
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

int ObDASIndexMergeAndIter::get_child_stores_last_valid_rowkey(uint64_t &last_valid_rowkey) const
{
  int ret = OB_SUCCESS;
  uint64_t min_rowkey = 0;
  uint64_t max_rowkey = UINT64_MAX;
  last_valid_rowkey = OB_LIKELY(!is_reverse_) ? UINT64_MAX : 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
    const IndexMergeRowStore &child_store = child_stores_.at(i);
    if (child_store.is_empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty child store", K(ret), K(i), K(child_store));
    } else if (OB_FAIL(child_store.get_min_max_rowkey(min_rowkey, max_rowkey))) {
      LOG_WARN("failed to get min max rowkey", K(ret), K(i), K(child_store));
    } else if (OB_LIKELY(!is_reverse_ && max_rowkey < last_valid_rowkey)) {
      last_valid_rowkey = max_rowkey;
    } else if (OB_UNLIKELY(is_reverse_ && min_rowkey > last_valid_rowkey)) {
      last_valid_rowkey = min_rowkey;
    }
  }

  if (OB_UNLIKELY(last_valid_rowkey == UINT64_MAX || last_valid_rowkey == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey", K(ret), K(last_valid_rowkey));
  }
  return ret;
}

int ObDASIndexMergeAndIter::locate_child_stores_to_valid_rowkey(uint64_t rowkey)
{
  int ret = OB_SUCCESS;
  bool finded = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
    IndexMergeRowStore &child_store = child_stores_.at(i);
    finded = false;
    if (child_store.is_empty()) {
    } else if (OB_FAIL(child_store.locate_rowkey(rowkey, finded))) {
      LOG_WARN("failed to locate rowkey", K(ret), K(i), K(child_store));
    } else if (finded) {
      if (OB_UNLIKELY(child_store.is_empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty child store", K(ret), K(i), K(child_store));
      } else {
        child_store.head_next();
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
