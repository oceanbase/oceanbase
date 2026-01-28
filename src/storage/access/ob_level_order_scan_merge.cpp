/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_level_order_scan_merge.h"

namespace oceanbase
{
namespace storage
{

int ObLevelOrderScanMerge::open(const blocksstable::ObDatumRange &range)
{
  int ret = OB_SUCCESS;

  // This merge is only used for no-order scan
  if (OB_UNLIKELY(!range.is_valid()
                  || access_ctx_->query_flag_.scan_order_ != ObQueryFlag::NoOrder)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid range or not no-order scan", KR(ret), K(range), KPC(access_ctx_));
  } else if (OB_FAIL(ObMultipleMerge::open())) {
    LOG_WARN("Fail to open ObMultipleMerge", KR(ret));
  } else {
    range_ = &range;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare())) {
    LOG_WARN("Fail to prepare ObMultipleMerge", KR(ret));
  } else if (OB_FAIL(construct_iters())) {
    LOG_WARN("Fail to construct iters", KR(ret));
  }

  return ret;
}

int ObLevelOrderScanMerge::prepare()
{
  int ret = OB_SUCCESS;

  curr_iter_idx_ = -1;
  is_unprojected_row_valid_ = false;
  unprojected_row_.reuse();

  return ret;
}

int ObLevelOrderScanMerge::set_all_blockscan(ObStoreRowIterator &iter)
{
  int ret = OB_SUCCESS;

  if (access_param_->iter_param_.enable_pd_blockscan() && iter.is_sstable_iter()) {
    // not performance critical, maybe shouldn't be singleton
    ObDatumRowkey border_key;
    border_key.set_max_rowkey();
    if (OB_UNLIKELY(access_ctx_->query_flag_.is_reverse_scan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected reverse scan", KR(ret), KPC(access_ctx_));
    } else if (OB_FAIL(iter.refresh_blockscan_checker(border_key))) {
      LOG_WARN("Fail to refresh blockscan checker", KR(ret));
    }
  }

  return ret;
}

int ObLevelOrderScanMerge::construct_iters()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(range_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Range is null", KR(ret));
  } else if (OB_FAIL(build_iter_array_for_all_tables())) {
    LOG_WARN("Fail to build iter array for all tables", KR(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); i++) {
    if (OB_FAIL(set_all_blockscan(*iters_.at(i)))) {
      LOG_WARN("Fail to set all blockscan", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    curr_iter_idx_ = 0;
    if (iters_.count() >= 1 && iters_.at(0)->can_blockscan()) {
      scan_state_ = ScanState::BATCH;
    }
  }

  return ret;
}

int ObLevelOrderScanMerge::build_iter(ObITable *table, const ObTableIterParam *iter_param, ObStoreRowIterator *&iter)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table) || OB_ISNULL(iter_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Table or iter param is null", KR(ret), KP(table), KP(iter_param));
  } else if (OB_FAIL(table->scan(*iter_param, *access_ctx_, *range_, iter))) {
    LOG_WARN("Fail to get iterator", KR(ret), KPC(table), K(*iter_param));
  }

  return ret;
}

int ObLevelOrderScanMerge::init_iter(ObITable *table, const ObTableIterParam *iter_param, ObStoreRowIterator *iter)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table) || OB_ISNULL(iter_param) || OB_ISNULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Table or iter param or iter is null", KR(ret), KP(table), KP(iter_param), KP(iter));
  } else if (OB_FAIL(iter->init(*iter_param, *access_ctx_, table, range_))) {
    LOG_WARN("Fail to init iter", KR(ret), KPC(table), K(*iter_param));
  }
  return ret;
}

int ObLevelOrderScanMerge::inner_get_next_rows()
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    bool can_batch = false;

    if (OB_UNLIKELY(curr_iter_idx_ < 0 || curr_iter_idx_ >= iters_.count())) {
      ret = OB_ITER_END;
      LOG_DEBUG("Iter end", KR(ret), K(curr_iter_idx_));
    } else if (OB_FAIL(can_batch_scan(can_batch))) {
      LOG_WARN("Fail to check can batch scan", KR(ret));
    } else if (!can_batch) {
      ret = OB_PUSHDOWN_STATUS_CHANGED;
      LOG_DEBUG("Can't batch scan, push down status changed", KR(ret), K(curr_iter_idx_));
    } else if (OB_FAIL(iters_.at(curr_iter_idx_)->get_next_rows())) {
      if (OB_UNLIKELY(OB_ITER_END != ret && OB_PUSHDOWN_STATUS_CHANGED != ret)) {
        LOG_WARN("Fail to get next rows", KR(ret), K(curr_iter_idx_));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        curr_iter_idx_++;
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObLevelOrderScanMerge::calc_scan_range()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(access_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Access context is not initialized", KR(ret));
  } else if (OB_UNLIKELY(access_ctx_->query_flag_.is_reverse_scan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected reverse scan", KR(ret), KPC(access_ctx_));
  } else {
    if (curr_rowkey_.is_valid()) {
      if (range_ != &cow_range_) {
        cow_range_ = *range_;
        range_ = &cow_range_;
      }
      cow_range_.change_boundary(curr_rowkey_, false /** is_reverse_scan */);
      if (curr_rowkey_.is_max_rowkey()) {
        cow_range_.end_key_.set_max_rowkey();
      }
    }
  }

  return ret;
}

int ObLevelOrderScanMerge::inner_get_next_row(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    if (curr_iter_idx_ < 0 || curr_iter_idx_ >= iters_.count()) {
      ret = OB_ITER_END;
    } else {
      ObStoreRowIterator *iter = iters_.at(curr_iter_idx_);
      const ObDatumRow *row_ptr = nullptr;

      if (OB_FAIL(iter->get_next_row(row_ptr))) {
        if (OB_ITER_END == ret) {
          curr_iter_idx_++;
          ret = OB_SUCCESS;
        } else if (OB_PUSHDOWN_STATUS_CHANGED != ret) {
          LOG_WARN("Fail to get next row", KR(ret), K(curr_iter_idx_));
        }
      } else if (OB_FAIL(row.shallow_copy_with_local_storage_datum(*row_ptr))) {
        LOG_WARN("Fail to shallow copy row", KR(ret));
      } else {
        if (OB_NOT_NULL(range_)) {
          row.group_idx_ = range_->get_group_idx();
        }
        break;
      }
    }
  }

  return ret;
}

void ObLevelOrderScanMerge::reset()
{
  range_ = nullptr;
  cow_range_.reset();
  curr_iter_idx_ = -1;
  ObMultipleMerge::reset();
}

void ObLevelOrderScanMerge::reuse()
{
  ObMultipleMerge::reuse();
  curr_iter_idx_ = -1;
}

void ObLevelOrderScanMerge::reclaim()
{
  range_ = nullptr;
  curr_iter_idx_ = -1;
  ObMultipleMerge::reclaim();
}

int ObLevelOrderScanMerge::pause(bool& do_pause)
{
  do_pause = false;
  return OB_SUCCESS;
}

int ObLevelOrderScanMerge::can_batch_scan(bool &can_batch)
{
  int ret = OB_SUCCESS;

  can_batch = false;
  if (access_param_->iter_param_.enable_pd_filter()) {
    ObStoreRowIterator *iter = nullptr;
    if (OB_UNLIKELY(curr_iter_idx_ < 0 || curr_iter_idx_ >= iters_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected iter cnt", KR(ret), K(curr_iter_idx_), K(iters_.count()), K(*this));
    } else if (OB_ISNULL(iter = iters_.at(curr_iter_idx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null iter", KR(ret), K(curr_iter_idx_), K(iters_), K(*this));
    } else if (iter->can_batch_scan()) {
      can_batch = true;
    }
  }

  return ret;
}

void ObLevelOrderMultiScanMerge::reset()
{
  ObLevelOrderScanMerge::reset();
  ranges_ = nullptr;
  cow_ranges_.reset();
}

int ObLevelOrderMultiScanMerge::open(const ObIArray<ObDatumRange> &ranges)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ranges.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid ranges", KR(ret), K(ranges));
  } else if (OB_FAIL(ObMultipleMerge::open())) {
    LOG_WARN("Fail to open ObMultipleMerge", KR(ret));
  } else {
    ranges_ = &ranges;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare())) {
    LOG_WARN("Fail to prepare ObMultipleMerge", KR(ret));
  } else if (OB_FAIL(construct_iters())) {
    LOG_WARN("Fail to construct iters", KR(ret));
  }

  return ret;
}

int ObLevelOrderMultiScanMerge::calc_scan_range()
{
  int ret = OB_SUCCESS;

  const ObITableReadInfo *read_info = nullptr;

  if (!curr_rowkey_.is_valid()) {
    // no row has been iterated
  } else if (OB_ISNULL(access_param_) || OB_ISNULL(access_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLevelOrderMultiScanMerge not inited", KR(ret), KP(access_param_), KP(access_ctx_));
  } else if (OB_ISNULL(ranges_) || OB_ISNULL(read_info = access_param_->iter_param_.get_read_info())
             || OB_UNLIKELY(access_ctx_->query_flag_.is_reverse_scan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Ranges or read info is null or reverse scan",
             KR(ret),
             KP(ranges_),
             KP(read_info),
             KPC(access_ctx_));
  } else {
    ObSEArray<ObDatumRange, 8> tmp_ranges;
    if (OB_FAIL(tmp_ranges.reserve(ranges_->count()))) {
      LOG_WARN("Fail to reserve memory for range array", KR(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < ranges_->count(); ++i) {
      if (OB_FAIL(tmp_ranges.push_back(ranges_->at(i)))) {
        LOG_WARN("Fail to push back range", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (ranges_ != &cow_ranges_) {
        ranges_ = &cow_ranges_;
      }

      cow_ranges_.reset();
      for (int64_t i = curr_scan_index_; OB_SUCC(ret) && i < tmp_ranges.count(); i++) {
        ObDatumRange &range = tmp_ranges.at(i);
        if (curr_scan_index_ == i) {
          int cmp_ret = 0;
          const ObDatumRowkey &range_key = range.get_end_key();
          if (OB_FAIL(range_key.compare(curr_rowkey_, read_info->get_datum_utils(), cmp_ret))) {
            LOG_WARN("Fail to compare range key", KR(ret), K(range_key), K(curr_rowkey_));
          } else if (cmp_ret > 0) {
            range.change_boundary(curr_rowkey_, false /** is_reverse_scan */);
            if (curr_rowkey_.is_max_rowkey()) {
              range.end_key_.set_max_rowkey();
            }
            if (OB_FAIL(cow_ranges_.push_back(range))) {
              LOG_WARN("Fail to push back range", KR(ret));
            } else {
              range_idx_delta_ += i;
            }
          } else {
            range_idx_delta_ += (i + 1);
          }
        } else if (OB_FAIL(cow_ranges_.push_back(range))) {
          LOG_WARN("Fail to push back range", KR(ret));
        }
      }
    }
  }

  return ret;
}

int ObLevelOrderMultiScanMerge::construct_iters()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 == ranges_->count())) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(build_iter_array_for_all_tables())) {
    LOG_WARN("Fail to build iter array for all tables", KR(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); i++) {
    if (OB_FAIL(set_all_blockscan(*iters_.at(i)))) {
      LOG_WARN("Fail to set all blockscan", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    curr_iter_idx_ = 0;
    if (iters_.count() >= 1 && iters_.at(0)->can_blockscan()) {
      scan_state_ = ScanState::BATCH;
    }
  }

  return ret;
}

int ObLevelOrderMultiScanMerge::inner_get_next_row(ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObLevelOrderScanMerge::inner_get_next_row(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret && OB_PUSHDOWN_STATUS_CHANGED != ret)) {
      LOG_WARN("Fail to get next row", KR(ret), K(row), KPC(ranges_));
    }
  } else {
    row.group_idx_ = ranges_->at(row.scan_index_).get_group_idx();
  }

  return ret;
}

int ObLevelOrderMultiScanMerge::build_iter(ObITable *table, const ObTableIterParam *iter_param, ObStoreRowIterator *&iter)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table) || OB_ISNULL(iter_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Table or iter param is null", KR(ret), KP(table), KP(iter_param));
  } else if (OB_FAIL(table->multi_scan(*iter_param, *access_ctx_, *ranges_, iter))) {
    LOG_WARN("Fail to get iterator", KR(ret), KPC(table), K(*iter_param));
  }

  return ret;
}

int ObLevelOrderMultiScanMerge::init_iter(ObITable *table, const ObTableIterParam *iter_param, ObStoreRowIterator *iter)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table) || OB_ISNULL(iter_param) || OB_ISNULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Table or iter param or iter is null", KR(ret), KP(table), KP(iter_param), KP(iter));
  } else if (OB_FAIL(iter->init(*iter_param, *access_ctx_, table, ranges_))) {
    LOG_WARN("Fail to init iter", KR(ret), KPC(table), K(*iter_param));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
