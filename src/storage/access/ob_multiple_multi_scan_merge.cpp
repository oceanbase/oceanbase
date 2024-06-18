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

#include "ob_multiple_multi_scan_merge.h"
#include <math.h>
#include "storage/ob_row_fuse.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/ob_storage_schema.h"

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
  ObMultipleScanMerge::reset();
  ranges_ = NULL;
  cow_ranges_.reset();
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
  const ObITableReadInfo *read_info = nullptr;

  if (!curr_rowkey_.is_valid()) {
    // no row has been iterated
  } else if (NULL == access_param_ || NULL == access_ctx_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multiple multi scan merge not inited", K(ret), KP(access_param_), KP(access_ctx_));
  } else if (OB_ISNULL(ranges_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ranges is NULL", K(ret));
  } else if (OB_ISNULL(read_info = access_param_->iter_param_.get_read_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null read info", K(ret));
  } else {
    ObSEArray<ObDatumRange, 32> tmp_ranges;
    if (OB_FAIL(tmp_ranges.reserve(ranges_->count()))) {
      STORAGE_LOG(WARN, "fail to reserve memory for range array", K(ret));
    }
    for (int64_t i = 0; i < ranges_->count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(tmp_ranges.push_back(ranges_->at(i)))) {
        STORAGE_LOG(WARN, "fail to push back range", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (ranges_ != &cow_ranges_) {
        cow_ranges_.reset();
        ranges_ = &cow_ranges_;
      }
    }

    if (OB_SUCC(ret)) {
      const bool is_reverse_scan = access_ctx_->query_flag_.is_reverse_scan();
      int64_t l = curr_scan_index_;
      int64_t r = tmp_ranges.count();

      cow_ranges_.reuse();
      for (int64_t i = l; i < r && OB_SUCC(ret); ++i) {
        ObDatumRange &range = tmp_ranges.at(i);
        if (curr_scan_index_ == i) {
          int cmp_ret = 0;
          bool include_rowkey = false;
          const ObDatumRowkey &range_key = is_reverse_scan ? range.get_start_key() : range.get_end_key();
          if (OB_FAIL(range_key.compare(curr_rowkey_, read_info->get_datum_utils(), cmp_ret))) {
            STORAGE_LOG(WARN, "Failed to cmopare range key ", K(ret), K(range_key), K(curr_rowkey_));
          } else if ((is_reverse_scan && cmp_ret < 0) || (!is_reverse_scan && cmp_ret > 0)) {
            range.change_boundary(curr_rowkey_, is_reverse_scan);
            range_idx_delta_ += i;
            if (OB_FAIL(cow_ranges_.push_back(range))) {
              STORAGE_LOG(WARN, "push back range failed", K(ret));
            }
          } else {
            range_idx_delta_ += i + 1;
          }
        } else if (OB_FAIL(cow_ranges_.push_back(range))) {
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
  if (OB_ISNULL(ranges_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ranges is null", K(ret));
  } else if (0 == ranges_->count()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObMultipleMultiScanMerge::construct_iters()
{
  int ret = OB_SUCCESS;

  consumer_cnt_ = 0;
  if (OB_UNLIKELY(iters_.count() > 0 && iters_.count() != tables_.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter cnt is not equal to table cnt", K(ret), "iter cnt", iters_.count(),
        "table cnt", tables_.count(), KP(this));
  } else if (tables_.count() > 0) {
    ObITable *table = NULL;
    ObStoreRowIterator *iter = NULL;
    const ObTableIterParam *iter_param = NULL;
    bool use_cache_iter = iters_.count() > 0;

    if (OB_FAIL(set_rows_merger(tables_.count()))) {
      STORAGE_LOG(WARN, "Failed to alloc rows merger", K(ret));
    }

    for (int64_t i = tables_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (OB_FAIL(tables_.at(i, table))) {
        STORAGE_LOG(WARN, "Fail to get i store, ", K(i), K(ret));
      } else if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Fail to get access param", K(i), K(ret), K(*table));
      } else if (!use_cache_iter) {
        if (OB_FAIL(table->multi_scan(*iter_param, *access_ctx_, *ranges_, iter))) {
          STORAGE_LOG(WARN, "Fail to get iterator, ", K(ret), K(i), K(*iter_param));
        } else if (OB_FAIL(iters_.push_back(iter))) {
          iter->~ObStoreRowIterator();
          STORAGE_LOG(WARN, "Fail to push iter to iterator array, ", K(ret), K(i));
        }
      } else if (OB_ISNULL(iter = iters_.at(tables_.count() - 1 - i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null iter", K(ret), "idx", tables_.count() - 1 - i, K_(iters));
      } else if (OB_FAIL(iter->init(*iter_param, *access_ctx_, table, ranges_))) {
        STORAGE_LOG(WARN, "failed to init scan iter", K(ret), "idx", tables_.count() - i);
      }

      if (OB_SUCC(ret)) {
        consumers_[consumer_cnt_++] = i;
        STORAGE_LOG(DEBUG, "add iter for consumer", KPC(table), KPC(access_param_));
      }
    }

    if (OB_SUCC(ret) && access_param_->iter_param_.enable_pd_blockscan() &&
        consumer_cnt_ > 0 && nullptr != iters_.at(consumers_[0]) && iters_.at(consumers_[0])->is_sstable_iter() &&
        OB_FAIL(locate_blockscan_border())) {
      STORAGE_LOG(WARN, "Fail to locate blockscan border", K(ret));
    }
  }

  return ret;
}

int ObMultipleMultiScanMerge::inner_get_next_row(blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObMultipleScanMerge::inner_get_next_row(row))) {
    row.group_idx_ = ranges_->at(row.scan_index_).get_group_idx();
    STORAGE_LOG(DEBUG, "multi_scan_merge: get_next_row", K(row));
  }
  return ret;
}

}
}
#endif
