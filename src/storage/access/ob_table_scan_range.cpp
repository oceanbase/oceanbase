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

#include "ob_table_scan_range.h"
#include "ob_dml_param.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

ObTableScanRange::ObTableScanRange()
  : rowkeys_(),
    ranges_(),
    skip_scan_ranges_(),
    allocator_(nullptr),
    status_(EMPTY),
    is_inited_(false)
{
  rowkeys_.set_attr(ObMemAttr(MTL_ID(), "TScanRowkeys"));
  ranges_.set_attr(ObMemAttr(MTL_ID(), "TScanRanges"));
  skip_scan_ranges_.set_attr(ObMemAttr(MTL_ID(), "TScanSSRanges"));
}

void ObTableScanRange::reset()
{
#define RESET_SCAN_RANGES(RANGES)                                                    \
do {                                                                                 \
  for (int64_t i = 0; i < RANGES.count(); i++) {                                     \
    ObDatumRange &range = RANGES.at(i);                                              \
    if (!range.get_start_key().is_static_rowkey()) {                                 \
      allocator_->free(const_cast<ObStorageDatum *>(range.get_start_key().datums_)); \
    }                                                                                \
    if (!range.get_end_key().is_static_rowkey()) {                                   \
      allocator_->free(const_cast<ObStorageDatum *>(range.get_end_key().datums_));   \
    }                                                                                \
  }                                                                                  \
} while(0)                                                                           \

  if (nullptr != allocator_) {
    RESET_SCAN_RANGES(ranges_);
    RESET_SCAN_RANGES(skip_scan_ranges_);

    for (int64_t i = 0; i < rowkeys_.count(); i++) {
      if (!rowkeys_.at(i).is_static_rowkey()) {
        allocator_->free(const_cast<ObStorageDatum *>(rowkeys_.at(i).datums_));
      }
    }
  }
  rowkeys_.reset();
  ranges_.reset();
  skip_scan_ranges_.reset();
  allocator_ = nullptr;
  status_ = EMPTY;
  is_inited_ = false;
}

int ObTableScanRange::init(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTableScanRange is inited twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(!scan_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObTableScanRange", K(ret), K(scan_param));
  } else {
    allocator_ = scan_param.scan_allocator_;
    status_ = scan_param.is_get_ ? GET : SCAN;
    const ObStorageDatumUtils *datum_utils  = nullptr;
    datum_utils = &scan_param.table_param_->get_read_info().get_datum_utils();
    if (OB_UNLIKELY(!datum_utils->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error for invalid datum utils", K(ret), KPC(scan_param.table_param_));
    } else if (scan_param.is_get_) {
      if (scan_param.use_index_skip_scan()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected, index skip scan can only be used in scan", K(ret));
      } else if (OB_FAIL(init_rowkeys(scan_param.key_ranges_, scan_param.scan_flag_, datum_utils))) {
        STORAGE_LOG(WARN, "Failed to init rowkeys", K(ret));
      }
    } else if (scan_param.use_index_skip_scan()) {
      if (OB_FAIL(init_ranges_in_skip_scan(scan_param.key_ranges_, scan_param.ss_key_ranges_, scan_param.scan_flag_, datum_utils))) {
        STORAGE_LOG(WARN, "Failed to init range in skip scan", K(ret), K(scan_param.key_ranges_), K(scan_param.ss_key_ranges_));
      }
    } else if (OB_FAIL(init_ranges(scan_param.key_ranges_, scan_param.scan_flag_, datum_utils))) {
      STORAGE_LOG(WARN, "Failed to init ranges", K(ret));
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
      STORAGE_LOG(DEBUG, "Succ to init table scan range", K(*this), K(scan_param));
    }
  }

  return ret;
}

int ObTableScanRange::init(const ObSimpleBatch &simple_batch, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObQueryFlag scan_flag;
  scan_flag.scan_order_ = ObQueryFlag::Forward;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTableScanRange is not inited", K(ret), K(*this));
  } else if (OB_UNLIKELY(!simple_batch.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init table scan range", K(ret), K(simple_batch));
  } else if (FALSE_IT(allocator_ = &allocator)) {
  } else if (simple_batch.type_ == ObSimpleBatch::T_SCAN) {
    //single scan
    ObSEArray<ObNewRange, 1> ranges;
    if (OB_ISNULL(simple_batch.ranges_)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid simple batch", K(ret), K(simple_batch));
    } else if (OB_FAIL(ranges.push_back(*simple_batch.range_))) {
      STORAGE_LOG(WARN, "Failed to push back range", K(ret));
    } else if (OB_FAIL(init_ranges(ranges, scan_flag, nullptr))) {
      STORAGE_LOG(WARN, "Failed to init ranges", K(ret));
    }
    //multiple scan
  } else if (OB_ISNULL(simple_batch.ranges_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid simple batch", K(ret), K(simple_batch));
  } else if (OB_FAIL(init_ranges(*simple_batch.ranges_, scan_flag, nullptr))) {
    STORAGE_LOG(WARN, "Failed to init ranges", K(ret));
  }
  if (OB_SUCC(ret)) {
    status_ = SCAN;
    is_inited_ = true;
    STORAGE_LOG(DEBUG, "Succ to init table scan range", K(*this), K(simple_batch));
  }

  return ret;
}

int ObTableScanRange::always_false(const common::ObNewRange &range, bool &is_false)
{
  int ret = OB_SUCCESS;
  int cmp = 0;

  if (OB_FAIL(range.get_start_key().compare(range.get_end_key(), cmp))) {
    STORAGE_LOG(WARN, "Failed to compare range keys", K(ret), K(range));
  } else {
    is_false = (cmp > 0) || (0 == cmp && (!range.border_flag_.inclusive_start() || !range.border_flag_.inclusive_end()));
    if (is_false) {
      STORAGE_LOG(DEBUG, "chaser debug always false range", K(ret), K(range), K(range.border_flag_));
    }
  }
  return ret;
}

int ObTableScanRange::init_rowkeys(const common::ObIArray<common::ObNewRange> &ranges,
                                   const common::ObQueryFlag &scan_flag,
                                   const blocksstable::ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init rowkeys", K(ret));
  } else {
    const int64_t range_cnt = ranges.count();
    if (range_cnt == 0) {
      status_ = EMPTY;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < range_cnt; i++) {
        ObDatumRowkey datum_rowkey;
        const ObRowkey &rowkey = ranges.at(i).get_start_key();
        bool is_false = false;
        if (OB_FAIL(always_false(ranges.at(i), is_false))) {
          STORAGE_LOG(WARN, "Failed to check range always false", K(ret), K(ranges.at(i)));
        } else if (is_false) {
        } else if (OB_FAIL(datum_rowkey.from_rowkey(rowkey, *allocator_))) {
          STORAGE_LOG(WARN, "Failed to transfer rowkey to datum rowkey", K(ret));
        } else if (FALSE_IT(datum_rowkey.set_group_idx(ranges.at(i).get_group_idx()))) {
        } else if (OB_FAIL(rowkeys_.push_back(datum_rowkey))) {
          STORAGE_LOG(WARN, "Failed to push back datum rowkey", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (rowkeys_.empty()) {
          status_ = EMPTY;
        } else if (rowkeys_.count() > 1 && nullptr != datum_utils && scan_flag.is_ordered_scan()) {
          ObDatumComparor<ObDatumRowkey> comparor(*datum_utils, ret, scan_flag.is_reverse_scan());
          std::sort(rowkeys_.begin(), rowkeys_.end(), comparor);
          if (OB_FAIL(ret)) {
            STORAGE_LOG(WARN, "Failed to sort datum rowkeys", K(ret), K(rowkeys_));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableScanRange::init_ranges(const common::ObIArray<common::ObNewRange> &ranges,
                                  const common::ObQueryFlag &scan_flag,
                                  const blocksstable::ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ranges", K(ret));
  } else {
    const int64_t range_cnt = ranges.count();
    if (0 == range_cnt) {
      ObDatumRange datum_range;
      datum_range.set_whole_range();
      if (OB_FAIL(ranges_.push_back(datum_range))) {
        STORAGE_LOG(WARN, "Failed to push back datum range", K(ret));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < range_cnt; i++) {
        ObDatumRange datum_range;
        const ObNewRange &range = ranges.at(i);
        bool is_false = false;
        if (OB_FAIL(always_false(range, is_false))) {
          STORAGE_LOG(WARN, "Failed to check range always false", K(ret), K(range));
        } else if (is_false) {
        } else if (OB_FAIL(datum_range.from_range(range, *allocator_))) {
          STORAGE_LOG(WARN, "Failed to transfer range to datum range", K(ret));
        } else if (OB_FAIL(ranges_.push_back(datum_range))) {
          STORAGE_LOG(WARN, "Failed to push back datum range", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (ranges_.empty()) {
          status_ = EMPTY;
        } else if (ranges_.count() > 1 && nullptr != datum_utils && scan_flag.is_ordered_scan()) {
          ObDatumComparor<ObDatumRange> comparor(*datum_utils, ret, scan_flag.is_reverse_scan());
          std::sort(ranges_.begin(), ranges_.end(), comparor);
          if (OB_FAIL(ret)) {
            STORAGE_LOG(WARN, "Failed to sort datum ranges", K(ret), K(ranges_));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableScanRange::init_ranges_in_skip_scan(const common::ObIArray<common::ObNewRange> &ranges,
                                               const common::ObIArray<common::ObNewRange> &skip_scan_ranges,
                                               const common::ObQueryFlag &scan_flag,
                                               const blocksstable::ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == allocator_ ||
      ranges.count() != skip_scan_ranges.count()) ||
      ranges.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ranges", K(ret), K(allocator_), K(ranges.count()), K(skip_scan_ranges.count()));
  } else {
    common::ObSEArray<ObSkipScanWrappedRange, DEFAULT_RANGE_CNT> wrapped_ranges_;
    const int64_t range_cnt = ranges.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < range_cnt; i++) {
      ObSkipScanWrappedRange wrapped_range;
      const ObNewRange &range = ranges.at(i);
      const ObNewRange &skip_scan_range = skip_scan_ranges.at(i);
      bool is_false = false;
      if (OB_FAIL(always_false(range, is_false))) {
        STORAGE_LOG(WARN, "Failed to check range always false", K(ret), K(range));
      } else if (is_false) {
      } else if (OB_FAIL(wrapped_range.datum_range_.from_range(range, *allocator_))) {
        STORAGE_LOG(WARN, "Failed to transfer range to datum range", K(ret));
      } else if (OB_FAIL(wrapped_range.datum_skip_range_.from_range(skip_scan_range, *allocator_))) {
        STORAGE_LOG(WARN, "Failed to transfer skip range to datum range", K(ret));
      } else if (OB_FAIL(wrapped_ranges_.push_back(wrapped_range))) {
        STORAGE_LOG(WARN, "Failed to push back", K(ret), K(wrapped_range));
      }
    }
    if (OB_SUCC(ret)) {
      if (wrapped_ranges_.empty()) {
        status_ = EMPTY;
      } else if (wrapped_ranges_.count() > 1 && nullptr != datum_utils && scan_flag.is_ordered_scan()) {
        ObDatumComparor<ObSkipScanWrappedRange> comparor(*datum_utils, ret, scan_flag.is_reverse_scan());
        std::sort(wrapped_ranges_.begin(), wrapped_ranges_.end(), comparor);
        if (OB_FAIL(ret)) {
          STORAGE_LOG(WARN, "Failed to sort datum ranges", K(ret), K(wrapped_ranges_));
        }
      }
    }
    if (OB_SUCC(ret) && EMPTY != status_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < wrapped_ranges_.count(); i++) {
        const ObSkipScanWrappedRange &wrapped_range = wrapped_ranges_.at(i);
        STORAGE_LOG(DEBUG, "skip scan range", K(wrapped_range));
        if (OB_FAIL(ranges_.push_back(wrapped_range.datum_range_))) {
          STORAGE_LOG(WARN, "Failed to push back datum range", K(ret));
        } else if (OB_FAIL(skip_scan_ranges_.push_back(wrapped_range.datum_skip_range_))) {
          STORAGE_LOG(WARN, "Failed to push back datum range", K(ret));
        }
      }
    }
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase
