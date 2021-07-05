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

#include "storage/ob_range_iterator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/profile/ob_perf_event.h"
#include "common/ob_range.h"
#include "share/ob_simple_batch.h"
#include "storage/ob_dml_param.h"

namespace oceanbase {
using namespace common;
namespace storage {
int ObBatch::get_storage_batch(const ObSimpleBatch& sql_batch, ObIAllocator& allocator, ObBatch& batch)
{
  int ret = OB_SUCCESS;
  ObStoreRange store_range;
  batch.type_ = static_cast<ObBatch::ObBatchType>(sql_batch.type_);
  if (ObBatch::T_NONE == static_cast<ObBatch::ObBatchType>(sql_batch.type_)) {
    batch.rowkey_ = NULL;
  } else if (ObBatch::T_SCAN == static_cast<ObBatch::ObBatchType>(sql_batch.type_)) {
    ObExtStoreRange* ext_store_range = NULL;
    if (OB_ISNULL(sql_batch.range_)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "NULL range", K(ret), K(sql_batch));
    } else {
      store_range.assign(*sql_batch.range_);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ext_store_range = static_cast<ObExtStoreRange*>(allocator.alloc(sizeof(ObExtStoreRange))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate ObStoreRange", K(ret));
    } else {
      batch.range_ = new (ext_store_range) ObExtStoreRange(store_range);
      if (OB_FAIL(ext_store_range->to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
        STORAGE_LOG(WARN, "Failed to transform collation free and cutoff range", K(store_range), K(ret));
      }
    }
  } else if (ObBatch::T_MULTI_SCAN == static_cast<ObBatch::ObBatchType>(sql_batch.type_)) {
    ScanRangeArray* scan_range_arr = NULL;
    if (OB_ISNULL(sql_batch.ranges_)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "NULL ranges", K(ret), K(sql_batch));
    } else if (OB_ISNULL(scan_range_arr = static_cast<ScanRangeArray*>(allocator.alloc(sizeof(ScanRangeArray))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate ScanRangeArray", K(ret));
    } else {
      batch.ranges_ = new (scan_range_arr) ScanRangeArray();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_batch.ranges_->count(); i++) {
      store_range.assign(sql_batch.ranges_->at(i));
      if (OB_FAIL(scan_range_arr->push_back(ObExtStoreRange(store_range)))) {
        STORAGE_LOG(WARN, "Failed to push back store range", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_range_arr->count(); i++) {
      if (OB_FAIL(scan_range_arr->at(i).to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
        STORAGE_LOG(
            WARN, "Failed to transform collation free and cutoff range", K(i), K(scan_range_arr->at(i)), K(ret));
      }
    }
  } else {
    STORAGE_LOG(WARN, "invalid sql batch", K(sql_batch));
  }
  return ret;
}

OB_INLINE static bool order_specified(const ObTableScanParam* scan_param)
{
  return NULL != scan_param && scan_param->scan_flag_.scan_order_ > common::ObQueryFlag::ImplementedOrder;
}

OB_INLINE static int always_false(const ObStoreRange& range, const ObIArray<ObOrderType>* column_orders, bool& is_false)
{
  int ret = OB_SUCCESS;
  int32_t cmp = 0;

  if (OB_ISNULL(column_orders)) {
    cmp = range.get_start_key().compare(range.get_end_key());
  } else if (OB_FAIL(range.get_start_key().compare(range.get_end_key(), *column_orders, cmp))) {
    STORAGE_LOG(WARN, "start/end key comparison failed", K(ret), K(range.get_start_key()), K(range.get_end_key()));
  }
  if (OB_SUCC(ret)) {
    is_false = (cmp > 0) ||
               (0 == cmp && (!range.get_border_flag().inclusive_start() || !range.get_border_flag().inclusive_end()));
  }

  return ret;
}

class RangeCmp {
public:
  RangeCmp(const ObIArray<ObOrderType>* column_orders) : column_orders_(column_orders)
  {}

  // caller ensures no intersection between any two rangs
  OB_INLINE bool operator()(const ObExtStoreRange& a, const ObExtStoreRange& b)
  {
    int cmp = 0;
    if (OB_SUCCESS != a.get_range().compare_with_startkey(b.get_range(), column_orders_, cmp)) {
      right_to_die_or_duty_to_live();
    }
    return cmp < 0;
  }
  OB_INLINE bool operator()(const ObExtStoreRowkey& a, const ObExtStoreRowkey& b) const
  {
    int32_t cmp = 0;
    if (OB_LIKELY(NULL == column_orders_)) {
      cmp = a.get_store_rowkey().compare(b.get_store_rowkey());
    } else if (OB_SUCCESS != a.get_store_rowkey().compare(b.get_store_rowkey(), *column_orders_, cmp)) {
      right_to_die_or_duty_to_live();
    }
    return cmp < 0;
  }

private:
  const ObIArray<ObOrderType>* column_orders_;
};

void ObRangeIterator::reset()
{
  is_inited_ = false;
  scan_param_ = NULL;
  cur_idx_ = 0;
  order_ranges_.reset();
  rowkey_.reset();
  rowkeys_.reset();
  range_.reset();
  rowkey_column_orders_ = nullptr;
  rowkey_column_cnt_ = 0;
}

void ObRangeIterator::reuse()
{
  is_inited_ = false;
  scan_param_ = NULL;
  cur_idx_ = 0;
  order_ranges_.reuse();
  rowkey_.reset();
  rowkeys_.reuse();
  range_.reset();
  rowkey_column_orders_ = nullptr;
  rowkey_column_cnt_ = 0;
}

// set scan parameter and do range comparison currently
int ObRangeIterator::set_scan_param(ObTableScanParam& scan_param)
{
  int ret = OB_SUCCESS;
  int64_t array_cnt = scan_param.range_array_pos_.count();
  const int64_t range_cnt = scan_param.key_ranges_.count();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (FALSE_IT(rowkey_column_orders_ = scan_param.get_rowkey_column_orders())) {
    STORAGE_LOG(WARN, "get column order failed", K(ret));
  } else if (scan_param.is_get_) {
    if (OB_UNLIKELY(range_cnt <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid key ranges", K(ret), K(scan_param.key_ranges_));
    } else {
      scan_param_ = &scan_param;
      if (range_cnt > 1) {
        for (int64_t i = 0; OB_SUCC(ret) && i < range_cnt; ++i) {
          const ObRowkey& rowkey = scan_param_->key_ranges_.at(i).get_start_key();
          if (OB_FAIL(rowkeys_.push_back(
                  ObExtStoreRowkey(ObStoreRowkey(const_cast<ObObj*>(rowkey.get_obj_ptr()), rowkey.get_obj_cnt()))))) {
            STORAGE_LOG(WARN, "fail to push back rowkey", K(ret), K(rowkey));
          }
        }
      }
      if (OB_SUCC(ret) && array_cnt > 1) {
        int64_t curr_range_start_pos = 0;
        int64_t curr_range_end_pos = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < array_cnt; ++i) {
          curr_range_end_pos = scan_param.range_array_pos_.at(i) + 1;
          for (int64_t j = curr_range_start_pos; OB_SUCC(ret) && j < curr_range_end_pos; ++j) {
            set_range_array_idx(i, rowkeys_.at(j));
          }
          curr_range_start_pos = curr_range_end_pos;
        }
      }
    }
  } else {
    scan_param_ = &scan_param;
    array_cnt = 0 == array_cnt ? 1 : array_cnt;
    if (1 == array_cnt) {
      if (0 == range_cnt) {
        ObStoreRange whole_range;
        if (OB_FAIL(whole_range.set_whole_range(rowkey_column_orders_, *scan_param_->allocator_))) {
          STORAGE_LOG(WARN, "set_whole_range failed.", K(ret));
        } else if (OB_SUCCESS != (ret = order_ranges_.push_back(ObExtStoreRange(whole_range)))) {
          STORAGE_LOG(WARN, "push whole range error", K(ret));
        }
      } else {
        if (OB_FAIL(order_ranges_.reserve(range_cnt))) {
          STORAGE_LOG(WARN, "fail to reserve order ranges", K(ret), K(range_cnt));
        } else if (OB_FAIL(convert_key_ranges(0, range_cnt, 0, *scan_param_->allocator_, order_ranges_))) {
          STORAGE_LOG(WARN, "convert_key_ranges railed", K(ret));
        } else {
          // sort the ranges
          if ((ObQueryFlag::Forward == scan_param_->scan_flag_.scan_order_ ||
                  ObQueryFlag::Reverse == scan_param_->scan_flag_.scan_order_) &&
              range_cnt > 1) {
            std::sort(order_ranges_.begin(), order_ranges_.end(), RangeCmp(rowkey_column_orders_));
          }
        }
      }
    } else {
      int64_t range_array_idx = 0;
      int64_t curr_range_start_pos = 0;
      int64_t curr_range_end_pos = 0;
      if (ObQueryFlag::Forward == scan_param_->scan_flag_.scan_order_ ||
          ObQueryFlag::Reverse == scan_param_->scan_flag_.scan_order_) {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "sort multiple range array is not supported", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(order_ranges_.reserve(range_cnt))) {
        STORAGE_LOG(WARN, "fail to reserve order ranges", K(ret), K(range_cnt));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < array_cnt; ++i) {
        curr_range_end_pos = scan_param_->range_array_pos_.at(i) + 1;
        if (OB_FAIL(convert_key_ranges(
                curr_range_start_pos, curr_range_end_pos, range_array_idx, *scan_param_->allocator_, order_ranges_))) {
          STORAGE_LOG(WARN, "fail to convert key ranges", K(ret));
        } else {
          ++range_array_idx;
          curr_range_start_pos = curr_range_end_pos;
        }
      }
    }

    STORAGE_LOG(DEBUG,
        "set_scan_param returned",
        KP_(rowkey_column_orders),
        K(scan_param),
        K(scan_param_->key_ranges_),
        K(order_ranges_),
        K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(scan_param_)) {
    is_inited_ = true;
  } else {
    reset();
  }

  return ret;
}

int ObRangeIterator::convert_key_ranges(const int64_t range_begin_pos, const int64_t range_end_pos,
    const int64_t range_array_idx, ObIAllocator& allocator, ObIArray<ObExtStoreRange>& store_ranges)
{
  int ret = OB_SUCCESS;
  ObStoreRange store_range;
  for (int64_t i = range_begin_pos; OB_SUCC(ret) && i < range_end_pos; i++) {
    bool is_always_false = false;
    ObNewRange& cur_range = scan_param_->key_ranges_.at(i);
    if (OB_ISNULL(rowkey_column_orders_)) {
      store_range.assign(cur_range);
    } else if (OB_FAIL(cur_range.to_store_range(*rowkey_column_orders_, store_range, allocator))) {
      STORAGE_LOG(WARN, "convert to store range failed", K(ret), K(cur_range), K(rowkey_column_orders_));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(always_false(store_range, rowkey_column_orders_, is_always_false))) {
      STORAGE_LOG(WARN, "fail to check range always false", K(ret), K(store_range), K(rowkey_column_orders_));
    } else if (OB_UNLIKELY(is_always_false)) {
      STORAGE_LOG(INFO, "range is always false, skip it", K(store_range), K(rowkey_column_orders_));
    } else {
      ObExtStoreRange ext_range(store_range);
      set_range_array_idx(range_array_idx, ext_range);
      if (OB_FAIL(store_ranges.push_back(ext_range))) {
        STORAGE_LOG(WARN, "fail to push back store range", K(ret));
      }
    }
  }
  return ret;
}

int ObRangeIterator::get_next(ObBatch& batch)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  batch.type_ = ObBatch::T_NONE;

  bool is_always_false = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObRangeIterator is not initiated", K(ret));
  } else if (scan_param_->is_get_) {
    const int64_t range_count = scan_param_->key_ranges_.count();
    if (cur_idx_ >= range_count) {
      ret = OB_ITER_END;
    } else if (1 == range_count) {
      batch.type_ = ObBatch::T_GET;
      const ObRowkey& rowkey = scan_param_->key_ranges_.at(0).get_start_key();
      rowkey_ = ObExtStoreRowkey(ObStoreRowkey(const_cast<ObObj*>(rowkey.get_obj_ptr()), rowkey.get_obj_cnt()));
      batch.rowkey_ = &rowkey_;
      ++cur_idx_;
    } else {
      batch.type_ = ObBatch::T_MULTI_GET;

      const int64_t array_cnt = scan_param_->range_array_pos_.count();
      if ((0 == array_cnt || 1 == array_cnt) && (ObQueryFlag::Forward == scan_param_->scan_flag_.scan_order_ ||
                                                    ObQueryFlag::Reverse == scan_param_->scan_flag_.scan_order_)) {
        std::sort(rowkeys_.begin(), rowkeys_.end(), RangeCmp(rowkey_column_orders_));
      }
      if (OB_SUCC(ret)) {
        if (ObQueryFlag::Reverse == scan_param_->scan_flag_.scan_order_) {
          std::reverse(rowkeys_.begin(), rowkeys_.end());
        }
        batch.rowkeys_ = &rowkeys_;
        cur_idx_ += range_count;
      }
    }
  } else if (cur_idx_ >= (cnt = order_ranges_.count())) {
    ret = OB_ITER_END;
  } else if (1 == cnt) {
    const ObExtStoreRange& range = order_ranges_.at(cur_idx_++);
    if (OB_FAIL(always_false(range.get_range(), rowkey_column_orders_, is_always_false))) {
      STORAGE_LOG(WARN, "check range always false failed.", K(range), K(ret));
    } else if (is_always_false) {
      STORAGE_LOG(INFO, "range is always false, skip it", K(range));
      ret = OB_ITER_END;
    } else if (range.is_single_rowkey()) {
      batch.type_ = ObBatch::T_GET;
      rowkey_ = ObExtStoreRowkey(range.get_range().get_start_key());
      batch.rowkey_ = &rowkey_;
    } else {
      batch.type_ = ObBatch::T_SCAN;
      range_ = ObExtStoreRange(range);
      batch.range_ = &range_;
    }
    STORAGE_LOG(DEBUG, "next range", K(batch.type_), K(range), K(scan_param_->table_param_->get_table_id()));
  } else {
    // 1. skip always false range
    while (OB_SUCCESS == ret && cur_idx_ < cnt) {
      ObExtStoreRange& cur_ext_range = order_ranges_.at(cur_idx_);
      if (!cur_ext_range.get_range().is_single_rowkey()) {
        break;
      } else {
        ObExtStoreRowkey ext_rowkey(cur_ext_range.get_range().get_start_key());
        set_range_array_idx(cur_ext_range.get_range_array_idx(), ext_rowkey);
        if (OB_FAIL(rowkeys_.push_back(ext_rowkey))) {
          STORAGE_LOG(WARN, "fail to push back rowkey", K(ret));
        }
      }
      ++cur_idx_;
    }

    STORAGE_LOG(DEBUG, "range_iterator", "order", order_specified(scan_param_), "rowkey count", rowkeys_.count());
    if (OB_SUCC(ret)) {
      if (rowkeys_.count() > 0 && cur_idx_ == cnt) {
        // all ranges are get
        if (ObQueryFlag::Reverse == scan_param_->scan_flag_.scan_order_) {
          std::reverse(rowkeys_.begin(), rowkeys_.end());
        }
        batch.type_ = ObBatch::T_MULTI_GET;
        batch.rowkeys_ = &rowkeys_;
        STORAGE_LOG(DEBUG, "multi get", K(batch.type_), K(rowkeys_));
      } else {
        // use multi scan
        batch.type_ = ObBatch::T_MULTI_SCAN;
        batch.ranges_ = &order_ranges_;
        if (ObQueryFlag::Reverse == scan_param_->scan_flag_.scan_order_) {
          std::reverse(order_ranges_.begin(), order_ranges_.end());
        }
        STORAGE_LOG(DEBUG, "multi scan", K(batch.type_), K(order_ranges_));
      }
      cur_idx_ = scan_param_->key_ranges_.count();
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
