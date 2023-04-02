// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "ob_multiple_multi_skip_scan_merge.h"

namespace oceanbase
{
namespace storage
{

ObMultipleMultiSkipScanMerge::ObMultipleMultiSkipScanMerge()
  : cur_range_idx_(0),
    ranges_(nullptr),
    skip_scan_ranges_(nullptr)
{
}

ObMultipleMultiSkipScanMerge::~ObMultipleMultiSkipScanMerge()
{
  reset();
}

int ObMultipleMultiSkipScanMerge::init(
    const ObTableAccessParam &param,
    ObTableAccessContext &context,
    const ObGetTableParam &get_table_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMultipleSkipScanMerge::init(param, context, get_table_param))) {
    STORAGE_LOG(WARN, "Fail to init ObMultipleSkipScanMerge", K(ret), K(context), K(get_table_param));
  }
  return ret;
}

void ObMultipleMultiSkipScanMerge::reset()
{
  cur_range_idx_ = 0;
  ranges_ = nullptr;
  skip_scan_ranges_ = nullptr;
  ObMultipleSkipScanMerge::reset();
}

void ObMultipleMultiSkipScanMerge::reuse()
{
  cur_range_idx_ = 0;
  ranges_ = nullptr;
  skip_scan_ranges_ = nullptr;
  ObMultipleSkipScanMerge::reuse();

}

int ObMultipleMultiSkipScanMerge::open(
    const common::ObIArray<blocksstable::ObDatumRange> &ranges,
    const common::ObIArray<blocksstable::ObDatumRange> &skip_scan_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ranges.count() != skip_scan_ranges.count() || ranges.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), K(ranges.count()), K(skip_scan_ranges.count()));
  } else if (OB_FAIL(ObMultipleSkipScanMerge::open(ranges.at(cur_range_idx_), skip_scan_ranges.at(cur_range_idx_)))) {
    STORAGE_LOG(WARN, "Fail to open cur range", K(ret), K(cur_range_idx_));
  } else {
    ranges_ = &ranges;
    skip_scan_ranges_ = &skip_scan_ranges;
  }
  return ret;
}

int ObMultipleMultiSkipScanMerge::inner_get_next_row(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ObMultipleSkipScanMerge::inner_get_next_row(row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret && OB_PUSHDOWN_STATUS_CHANGED != ret)) {
        STORAGE_LOG(WARN, "Fail to inner get next row", K(ret), K(cur_range_idx_));
      } else if (OB_ITER_END == ret) {
        if (++cur_range_idx_ < ranges_->count()) {
          ret = OB_SUCCESS;
          ObMultipleSkipScanMerge::reuse();
          if (OB_FAIL(ObMultipleSkipScanMerge::open(ranges_->at(cur_range_idx_), skip_scan_ranges_->at(cur_range_idx_)))) {
            STORAGE_LOG(WARN, "Fail to open cur range", K(ret), K(cur_range_idx_));
          }
        }
      }
    } else {
      STORAGE_LOG(DEBUG, "get next row", K(row));
      break;
    }
  }
  return ret;
}

int ObMultipleMultiSkipScanMerge::inner_get_next_rows()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ObMultipleSkipScanMerge::inner_get_next_rows())) {
      if (OB_UNLIKELY(OB_ITER_END != ret && OB_PUSHDOWN_STATUS_CHANGED != ret)) {
        STORAGE_LOG(WARN, "Fail to inner get next row", K(ret), K(cur_range_idx_));
      } else if (OB_ITER_END == ret) {
        if (++cur_range_idx_ < ranges_->count()) {
          ret = OB_SUCCESS;
          ObMultipleSkipScanMerge::reuse();
          if (OB_FAIL(ObMultipleSkipScanMerge::open(ranges_->at(cur_range_idx_), skip_scan_ranges_->at(cur_range_idx_)))) {
            STORAGE_LOG(WARN, "Fail to open cur range", K(ret), K(cur_range_idx_));
          }
        }
      }
    } else {
      break;
    }
  }
  return ret;
}

}
}