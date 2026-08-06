/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  if (OB_FAIL(calc_scan_range_by_rowkey(access_param_,
                                        access_ctx_,
                                        ranges_,
                                        cow_ranges_,
                                        curr_scan_index_,
                                        curr_rowkey_,
                                        range_idx_delta_,
                                        false/*calc_di_base_range*/))) {
    STORAGE_LOG(WARN, "fail to calculate scan range", K(ret));
  } else {
    STORAGE_LOG(INFO, "calculate scan range", K(ret), KPC(ranges_), K(curr_scan_index_), K(curr_rowkey_));
    if (use_di_merge_scan()) {
      // 1. fast refresh table (RefreshTableState::NONE == refresh_table_state_)
      //   1.1 single di base is not changed, calculate its own scan_range
      //   1.2 di base empty before refresh table, can_calc_scan_range() = false, use same scan_range
      // 2. drain refresh table
      //    DI_BASE has caught up with the incremental data, use same scan_range
      if (RefreshTableState::NONE == refresh_table_state_ &&
          di_base_sstable_row_scanner_->can_calc_scan_range()) {
        if (OB_FAIL(di_base_sstable_row_scanner_->calc_scan_range())) {
          STORAGE_LOG(WARN, "fail to calculate di base scan range", K(ret), KPC(di_base_sstable_row_scanner_));
        }
      } else {
        if (OB_FAIL(di_base_sstable_row_scanner_->prepare_ranges(*ranges_, true/*copy_ranges*/))) {
          STORAGE_LOG(WARN, "fail to prepare di base ranges", K(ret), KPC(ranges_), KPC(di_base_sstable_row_scanner_));
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
