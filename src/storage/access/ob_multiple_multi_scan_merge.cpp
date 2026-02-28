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

#include "ob_multiple_multi_scan_merge.h"

#define USING_LOG_PREFIX STORAGE

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
    cow_ranges_()
{
  type_ = ObQRIterType::T_MULTI_SCAN;
}

ObMultipleMultiScanMerge::~ObMultipleMultiScanMerge()
{
}

void ObMultipleMultiScanMerge::reset()
{
  ranges_ = NULL;
  cow_ranges_.reset();
  ObMultipleScanMerge::reset();
}

int ObMultipleMultiScanMerge::open(const ObIArray<ObDatumRange> &ranges)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ranges.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid range count ", K(ret), K(ranges.count()));
  } else if (OB_FAIL(ObMultipleMerge::open())) {
    STORAGE_LOG(WARN, "Fail to open ObMultipleMerge, ", K(ret));
  } else if (OB_FAIL(prepare())) {
    STORAGE_LOG(WARN, "fail to prepare", K(ret));
  } else if (FALSE_IT(ranges_ = &ranges)) {
  } else if (use_di_merge_scan() && OB_FAIL(di_base_sstable_row_scanner_->prepare_ranges(ranges))) {
    STORAGE_LOG(WARN, "fail to prepare di base ranges", K(ret), K(ranges), KPC(di_base_sstable_row_scanner_));
  } else if (OB_FAIL(construct_iters())) {
    STORAGE_LOG(WARN, "fail to construct iters", K(ret));
  }

  return ret;
}

int ObMultipleMultiScanMerge::calc_scan_range()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_calc_scan_range(ranges_,
                                    cow_ranges_,
                                    curr_scan_index_,
                                    curr_rowkey_,
                                    false/*calc_di_base_range*/))) {
    STORAGE_LOG(WARN, "fail to calculate scan range", K(ret));
  } else {
    STORAGE_LOG(INFO, "calculate scan range", K(ret), KPC(ranges_), K(curr_scan_index_), K(curr_rowkey_));
    // calculate scan range for delete insert
    if (get_di_base_table_cnt() > 0) {
      if (OB_FAIL(inner_calc_scan_range(di_base_sstable_row_scanner_->get_di_base_multi_range(),
                                        di_base_sstable_row_scanner_->get_di_base_cow_multi_range(),
                                        di_base_sstable_row_scanner_->get_di_base_curr_scan_index(),
                                        di_base_sstable_row_scanner_->get_di_base_curr_rowkey(),
                                        true/*calc_di_base_range*/))) {
          STORAGE_LOG(WARN, "fail to calculate di base scan range", K(ret),
                                                                    KPC(di_base_sstable_row_scanner_->get_di_base_multi_range()),
                                                                    K(di_base_sstable_row_scanner_->get_di_base_curr_scan_index()),
                                                                    K(di_base_sstable_row_scanner_->get_di_base_curr_rowkey()));
        } else {
        STORAGE_LOG(INFO, "calculate di base range", K(ret),
                                                     KPC(di_base_sstable_row_scanner_->get_di_base_multi_range()),
                                                     K(di_base_sstable_row_scanner_->get_di_base_curr_scan_index()),
                                                     K(di_base_sstable_row_scanner_->get_di_base_curr_rowkey()));
      }
    }
  }
  return ret;
}

int ObMultipleMultiScanMerge::inner_calc_scan_range(const common::ObIArray<blocksstable::ObDatumRange> *&ranges,
                                                    common::ObIArray<blocksstable::ObDatumRange> &cow_ranges,
                                                    const int64_t curr_scan_index,
                                                    const blocksstable::ObDatumRowkey &curr_rowkey,
                                                    const bool calc_di_base_range)
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
            if (OB_UNLIKELY(!range.is_valid())) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "invalid range", K(ret), K(range));
            } else if (OB_FAIL(cow_ranges.push_back(range))) {
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

int ObMultipleMultiScanMerge::construct_iters()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 == ranges_->count())) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ObMultipleScanMerge::construct_iters())) {
    LOG_WARN("Fail to construct iters", KR(ret));
  }

  return ret;
}

int ObMultipleMultiScanMerge::build_iter(ObITable *table, const ObTableIterParam *iter_param, ObStoreRowIterator *&iter)
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

int ObMultipleMultiScanMerge::init_iter(ObITable *table, const ObTableIterParam *iter_param, ObStoreRowIterator *iter)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table) || OB_ISNULL(iter_param) || OB_ISNULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Table or iter param or iter is null", KR(ret), KP(table), KP(iter_param), KP(iter));
  } else if (OB_FAIL(iter->init(*iter_param, *access_ctx_, table, ranges_))) {
    LOG_WARN("Fail to init iterator", KR(ret), KPC(table), K(*iter_param));
  }

  return ret;
}

int ObMultipleMultiScanMerge::inner_get_next_row(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObMultipleScanMerge::inner_get_next_row(row))) {
    row.group_idx_ = ranges_->at(row.scan_index_).get_group_idx();
    STORAGE_LOG(DEBUG, "multi_scan_merge: get_next_row", K(row), KPC_(ranges));
  } else {
    STORAGE_LOG(DEBUG, "Failed to get next row from iterator", K(ret), KPC_(ranges));
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
