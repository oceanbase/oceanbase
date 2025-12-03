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

#define USING_LOG_PREFIX STORAGE

#include "storage/access/ob_multiple_multi_scan_merge.h"
#include "sql/engine/px/ob_granule_iterator_op.h"

#if !USE_NEW_MULTIPLE_MULTI_SCAN_MERGE
namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

ObMultipleMultiScanMerge::ObMultipleMultiScanMerge()
  : ObMultipleScanMerge(),
    ranges_(NULL),
    cow_ranges_(),
    di_base_ranges_(NULL),
    di_base_cow_ranges_()
{
  type_ = ObQRIterType::T_MULTI_SCAN;
}

ObMultipleMultiScanMerge::~ObMultipleMultiScanMerge()
{
}

void ObMultipleMultiScanMerge::reset()
{
  ObMultipleScanMerge::reset();
  ranges_ = NULL;
  cow_ranges_.reset();
  di_base_ranges_ = NULL;
  di_base_cow_ranges_.reset();
}

int ObMultipleMultiScanMerge::open(const ObIArray<ObDatumRange> &ranges)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ranges.count() <= 0)) {
    STORAGE_LOG(WARN, "Invalid range count ", K(ret), K(ranges.count()));
  } else if (OB_FAIL(ObMultipleMerge::open())) {
    STORAGE_LOG(WARN, "Fail to open ObMultipleMerge, ", K(ret));
  } else {
    ranges_ = &ranges;
    di_base_ranges_ = &ranges;
    if (OB_FAIL(ObMultipleMultiScanMerge::prepare())) {
      STORAGE_LOG(WARN, "fail to prepare", K(ret));
    } else if (OB_FAIL(construct_iters())) {
      STORAGE_LOG(WARN, "fail to construct iters", K(ret));
    }
  }

  return ret;
}

int ObMultipleMultiScanMerge::calc_scan_range()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(inner_calc_scan_range(ranges_, cow_ranges_, curr_scan_index_, curr_rowkey_, false))) {
    STORAGE_LOG(WARN, "fail to calculate scan range", K(ret));
  } else if (OB_FAIL(inner_calc_scan_range(di_base_ranges_, di_base_cow_ranges_, di_base_curr_scan_index_, di_base_curr_rowkey_, true))) {
    STORAGE_LOG(WARN, "fail to calculate di base scan range", K(ret));
  }
  return ret;
}

int ObMultipleMultiScanMerge::inner_calc_scan_range(const ObIArray<blocksstable::ObDatumRange> *&ranges,
                                                    common::ObSEArray<blocksstable::ObDatumRange, 32> &cow_ranges,
                                                    int64_t curr_scan_index,
                                                    blocksstable::ObDatumRowkey &curr_rowkey,
                                                    bool calc_di_base_range)
{
  int ret = OB_SUCCESS;
  const ObITableReadInfo *read_info = nullptr;

  if (!curr_rowkey.is_valid()) {
    // no row has been iterated
  } else if (NULL == access_param_ || NULL == access_ctx_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multiple multi scan merge not inited", K(ret), KP(access_param_), KP(access_ctx_));
  } else if (OB_ISNULL(ranges)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ranges is NULL", K(ret));
  } else if (OB_ISNULL(read_info = access_param_->iter_param_.get_read_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null read info", K(ret));
  } else {
    ObSEArray<ObDatumRange, 32> tmp_ranges;
    if (OB_FAIL(tmp_ranges.reserve(ranges->count()))) {
      STORAGE_LOG(WARN, "fail to reserve memory for range array", K(ret));
    }
    for (int64_t i = 0; i < ranges->count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(tmp_ranges.push_back(ranges->at(i)))) {
        STORAGE_LOG(WARN, "fail to push back range", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      const bool is_reverse_scan = access_ctx_->query_flag_.is_reverse_scan();
      int64_t l = curr_scan_index;
      int64_t r = tmp_ranges.count();

      if (ranges != &cow_ranges) {
        ranges = &cow_ranges;
      }
      cow_ranges.reset();
      for (int64_t i = l; i < r && OB_SUCC(ret); ++i) {
        ObDatumRange &range = tmp_ranges.at(i);
        if (curr_scan_index == i) {
          int cmp_ret = 0;
          const ObDatumRowkey &range_key = is_reverse_scan ? range.get_start_key() : range.get_end_key();
          if (OB_FAIL(range_key.compare(curr_rowkey, read_info->get_datum_utils(), cmp_ret))) {
            STORAGE_LOG(WARN, "Failed to cmopare range key ", K(ret), K(range_key), K(curr_rowkey));
            // notice, ranges should be pushed in when curr_rowkey is min/max
          } else if ((is_reverse_scan && cmp_ret < 0) || (!is_reverse_scan && cmp_ret > 0) ||
                     (((curr_scan_index + 1) == r) && access_param_->iter_param_.is_delete_insert_)) {
            range.change_boundary(curr_rowkey, is_reverse_scan, calc_di_base_range);
            // As memtable will use reverse scan when start rowkey is greater than end rowkey instead of
            // empty result, make the range correct
            if (access_ctx_->query_flag_.is_reverse_scan() && curr_rowkey.is_min_rowkey()) {
              range.start_key_.set_min_rowkey();
            } else if (!access_ctx_->query_flag_.is_reverse_scan() && curr_rowkey.is_max_rowkey()) {
              range.end_key_.set_max_rowkey();
            }
            if (OB_FAIL(cow_ranges.push_back(range))) {
              STORAGE_LOG(WARN, "push back range failed", K(ret));
            } else if (!calc_di_base_range) {
              range_idx_delta_ += i;
            }
          } else if (!calc_di_base_range) {
            range_idx_delta_ += (i + 1);
          }
        } else if (OB_FAIL(cow_ranges.push_back(range))) {
          STORAGE_LOG(WARN, "push back range failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObMultipleMultiScanMerge::is_range_valid() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ranges_) || OB_ISNULL(access_param_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ranges or di_base_ranges is null", K(ret), KP(ranges_), KP(access_param_));
  } else if (0 == ranges_->count() && !access_param_->iter_param_.is_delete_insert_) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObMultipleMultiScanMerge::construct_iters()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ranges_) || OB_ISNULL(di_base_ranges_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ranges or di_base_ranges is NULL", K(ret), KP(ranges_), KP(di_base_ranges_));
  } else if (OB_UNLIKELY(iters_.count() > 0 && iters_.count() + di_base_iters_.count() != tables_.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter cnt is not equal to table cnt", K(ret), "iter cnt", iters_.count(),
                "di_base_iter cnt", di_base_iters_.count(), "table cnt", tables_.count(), KP(this));
  } else if (OB_NOT_NULL(access_param_->get_op()) && access_param_->get_op()->is_vectorized() &&
             FALSE_IT(access_param_->get_op()->get_eval_ctx().reuse(access_param_->get_op()->get_batch_size()))) {
    // for check_skip_by_monotonicity called by initing iters in construct_iters
  } else if (tables_.count() > 0) {
    STORAGE_LOG(TRACE, "construct iters begin", K(tables_.count()), K(iters_.count()), K(di_base_iters_.count()),
                K(access_param_->iter_param_.is_delete_insert_), KPC_(ranges), KPC_(di_base_ranges), K_(access_ctx_->trans_version_range), K_(tables), KPC_(access_param));
    ObITable *table = NULL;
    ObStoreRowIterator *iter = NULL;
    const ObTableIterParam *iter_param = NULL;
    bool use_cache_iter = iters_.count() > 0 || di_base_iters_.count() > 0; // rescan with the same iters and different range

    if (access_param_->iter_param_.is_delete_insert_) {
      if (OB_FAIL(tables_.at(0, table))) {  // only one di base iter currently
        STORAGE_LOG(WARN, "Fail to get 0th store, ", K(ret), K_(tables));
      } else if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Fail to get 0th access param", K(ret), KPC(table));
      } else if (table->is_major_sstable()) {
        if (!use_cache_iter) {
          if (OB_FAIL(table->multi_scan(*iter_param, *access_ctx_, *di_base_ranges_, iter))) {
            STORAGE_LOG(WARN, "Fail to get di base iterator", K(ret), KPC(table), K(*iter_param));
          } else if (OB_FAIL(di_base_iters_.push_back(iter))) {
            iter->~ObStoreRowIterator();
            STORAGE_LOG(WARN, "Fail to push di base iter to di base iterator array", K(ret));
          }
        } else if (OB_ISNULL(iter = di_base_iters_.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null di_base_iters_", K(ret), "idx", 0, K(di_base_iters_));
        } else if (OB_FAIL(iter->init(*iter_param, *access_ctx_, table, di_base_ranges_))) {
          STORAGE_LOG(WARN, "failed to init scan di_base_iters_", K(ret), "idx", 0);
        }
        if (OB_SUCC(ret)) {
          STORAGE_LOG(DEBUG, "add di base iter for consumer", KPC(table));
        }
      }
    }

    consumer_cnt_ = 0;
    int32_t di_base_cnt = di_base_iters_.count();
    if (OB_FAIL(ret) || di_base_cnt == tables_.count()) {
    } else if (OB_FAIL(set_rows_merger(tables_.count() - di_base_cnt))) {
      STORAGE_LOG(WARN, "Failed to alloc rows merger", K(ret), K(di_base_cnt), K(tables_));
    } else {
      const int64_t table_cnt = tables_.count() - 1;
      for (int64_t i = table_cnt; OB_SUCC(ret) && i >= di_base_cnt; --i) {
        if (OB_FAIL(tables_.at(i, table))) {
          STORAGE_LOG(WARN, "Fail to get ith store, ", K(ret), K(i), K_(tables));
        } else if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Fail to get access param", K(ret), K(i), KPC(table));
        } else if (!use_cache_iter) {
          if (OB_FAIL(table->multi_scan(*iter_param, *access_ctx_, *ranges_, iter))) {
            STORAGE_LOG(WARN, "Fail to get iterator, ", K(ret), K(i), KPC(table), K(*iter_param));
          } else if (OB_FAIL(iters_.push_back(iter))) {
            iter->~ObStoreRowIterator();
            STORAGE_LOG(WARN, "Fail to push iter to iterator array, ", K(ret), K(i));
          }
        } else if (OB_ISNULL(iter = iters_.at(table_cnt - i))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected null iter", K(ret), "idx", table_cnt - i, K_(iters));
        } else if (OB_FAIL(iter->init(*iter_param, *access_ctx_, table, ranges_))) {
          STORAGE_LOG(WARN, "failed to init scan iter", K(ret), "idx", table_cnt - i);
        }

        if (OB_SUCC(ret)) {
          consumers_[consumer_cnt_++] = i - di_base_cnt;
          STORAGE_LOG(DEBUG, "add iter for consumer", K(i), KPC(table));
        }
      }
    }

    if (OB_SUCC(ret) && access_param_->iter_param_.enable_pd_blockscan()) {
      if (ScanState::DI_BASE == scan_state_) {
        if (OB_FAIL(get_di_base_iter()->refresh_blockscan_checker(curr_rowkey_))) {
          STORAGE_LOG(WARN, "Failed to refresh di base blockscan checker", K(ret), K(curr_rowkey_));
        }
      } else if (0 == consumer_cnt_ && 0 < di_base_iters_.count()) {
        if (OB_FAIL(prepare_di_base_blockscan(true))) {
          STORAGE_LOG(WARN, "Failed to prepare di base blockscan", K(ret));
        } else {
          scan_state_ = ScanState::DI_BASE;
        }
      } else if (consumer_cnt_ > 0 && nullptr != iters_.at(consumers_[0]) && iters_.at(consumers_[0])->is_sstable_iter()) {
        if (OB_FAIL(locate_blockscan_border())) {
          STORAGE_LOG(WARN, "Fail to locate blockscan border", K(ret), K(iters_.count()), K(di_base_iters_.count()), K_(tables));
        }
      }
    }
    STORAGE_LOG(DEBUG, "construct iters end", K(ret), K(iters_.count()), K(di_base_iters_.count()));
  }

  return ret;
}

int ObMultipleMultiScanMerge::inner_get_next_row(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObMultipleScanMerge::inner_get_next_row(row))) {
    row.group_idx_ = ranges_->at(row.scan_index_).get_group_idx();
    STORAGE_LOG(DEBUG, "multi_scan_merge: get_next_row", K(row), KPC_(ranges), KPC_(di_base_ranges));
  } else {
    STORAGE_LOG(DEBUG, "Failed to get next row from iterator", K(ret), KPC_(ranges), KPC_(di_base_ranges));
  }
  return ret;
}

int ObMultipleMultiScanMerge::pause(bool& do_pause)
{
  INIT_SUCC(ret);
  ScanResumePoint* scan_resume_point;
  const ObITableReadInfo* read_info;

  if (OB_FAIL(ObMultipleScanMerge::pause(do_pause))) {
    LOG_WARN("failed to pause");
  } else if (OB_LIKELY(!do_pause)) {
  } else {
    read_info = access_param_->iter_param_.get_read_info();
    scan_resume_point = access_ctx_->scan_resume_point_;
    // current range has been added in ObMultipleScanMerge::pause
    for (int64_t i = curr_scan_index_ + 1; i < ranges_->count(); ++i) {
      if (OB_FAIL(scan_resume_point->add_range(*read_info, ranges_->at(i)))) {
        STORAGE_LOG(WARN, "failed to add range");
        break;
      }
    }

    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO, "success to stop scan and save remain ranges", K(curr_rowkey_));
    } else {
      scan_resume_point->reset_ranges();
    }
  }
  return ret;
}


int ObMultipleMultiScanMerge::get_current_range(ObDatumRange& current_range) const
{
  INIT_SUCC(ret);
  if (OB_ISNULL(ranges_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ranges_ is null!");
  } else if (OB_FAIL(ranges_->at(curr_scan_index_, current_range))) {
    LOG_WARN("failed to get current range", K(curr_scan_index_), K(ranges_->count()));
  }
  return ret;
}

}
}
#endif
