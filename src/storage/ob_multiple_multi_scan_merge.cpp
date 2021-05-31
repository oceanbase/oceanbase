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

#include "storage/ob_multiple_multi_scan_merge.h"
#include <math.h>
#include "storage/ob_row_fuse.h"

#if !USE_NEW_MULTIPLE_MULTI_SCAN_MERGE
namespace oceanbase {
using namespace common;
namespace storage {

ObMultipleMultiScanMerge::ObMultipleMultiScanMerge()
    : ObMultipleScanMergeImpl(), get_num_(0), ranges_(NULL), cow_ranges_()
{
  memset(get_items_, 0, sizeof(ObStoreRow*) * MAX_TABLE_CNT_IN_STORAGE);
}

ObMultipleMultiScanMerge::~ObMultipleMultiScanMerge()
{}

int ObMultipleMultiScanMerge::is_get_data_ready(bool& is_ready)
{
  int ret = OB_SUCCESS;
  int64_t cur_get_index_ = -1;
  STORAGE_LOG(DEBUG, "calc_get_index", K(get_num_), K(iters_.count()));
  if (get_num_ == iters_.count()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
      if (NULL == get_items_[i]) {
        cur_get_index_ = -1;
        break;
      } else if (cur_get_index_ != -1 && cur_get_index_ != get_items_[i]->scan_index_) {
        cur_get_index_ = -1;
        break;
      } else {
        cur_get_index_ = get_items_[i]->scan_index_;
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_ready = cur_get_index_ != -1;
  }
  return ret;
}

int ObMultipleMultiScanMerge::is_scan_data_ready(bool& is_ready)
{
  int ret = OB_SUCCESS;
  is_ready = !is_scan_end();
  return ret;
}

void ObMultipleMultiScanMerge::reset()
{
  ObMultipleScanMergeImpl::reset();
  get_num_ = 0;
  ranges_ = NULL;
  cow_ranges_.reset();
  memset(get_items_, 0, sizeof(ObStoreRow*) * MAX_TABLE_CNT_IN_STORAGE);
}

int ObMultipleMultiScanMerge::open(const ObIArray<ObExtStoreRange>& ranges)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ranges.count() <= 0)) {
    STORAGE_LOG(WARN, "Invalid range count ", K(ret), K(ranges.count()));
  } else if (OB_FAIL(ObMultipleMerge::open())) {
    STORAGE_LOG(WARN, "Fail to open ObMultipleMerge, ", K(ret));
  } else if (OB_FAIL(to_collation_free_range_on_demand(ranges, *access_ctx_->allocator_))) {
    STORAGE_LOG(WARN, "fail to get collation free rowkey", K(ret));
  } else {
    row_filter_ = NULL;

    if (NULL != access_ctx_->row_filter_ && tables_handle_.has_split_source_table(access_ctx_->pkey_)) {
      row_filter_ = access_ctx_->row_filter_;
    }
    ranges_ = &ranges;
    if (OB_FAIL(ObMultipleMultiScanMerge::prepare())) {
      STORAGE_LOG(WARN, "fail to prepare", K(ret));
    } else if (OB_FAIL(construct_iters())) {
      STORAGE_LOG(WARN, "fail to construct iters", K(ret));
    } else if (OB_UNLIKELY(access_ctx_->need_prewarm())) {
      access_ctx_->store_ctx_->warm_up_ctx_->record_multi_scan(*access_param_, *access_ctx_, ranges);
    }
  }

  return ret;
}

int ObMultipleMultiScanMerge::calc_scan_range()
{
  int ret = OB_SUCCESS;
  ObStoreRange rowkey_range;

  if (!curr_rowkey_.is_valid()) {
    // no row has been iterated
  } else if (NULL == access_param_ || NULL == access_ctx_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multiple multi scan merge not inited", K(ret), KP(access_param_), KP(access_ctx_));
  } else if (OB_ISNULL(ranges_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ranges is NULL", K(ret));
  } else if (OB_FAIL(rowkey_range.build_range(access_param_->iter_param_.table_id_, curr_rowkey_))) {
    STORAGE_LOG(WARN, "fail to build range for rowkey", K(ret));
  } else {
    ObArray<ObExtStoreRange> tmp_ranges;
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
        ObExtStoreRange& range = tmp_ranges.at(i);

        if (curr_scan_index_ == i && range.get_range().include(rowkey_range)) {
          range.change_boundary(curr_rowkey_, is_reverse_scan, true);
          if (range.get_range().is_valid()) {
            if (OB_FAIL(const_cast<ObExtStoreRange&>(range).to_collation_free_range_on_demand_and_cutoff_range(
                    *access_ctx_->allocator_))) {
              STORAGE_LOG(WARN, "fail to get collation free rowkey", K(ret));
            } else if (OB_FAIL(cow_ranges_.push_back(range))) {
              STORAGE_LOG(WARN, "push back range failed", K(ret));
            } else {
              range_idx_delta_ += i;
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

OB_INLINE int ObMultipleMultiScanMerge::prepare()
{
  get_num_ = 0;
  return ObMultipleScanMergeImpl::prepare_loser_tree();
}

void ObMultipleMultiScanMerge::collect_merge_stat(ObTableStoreStat& stat) const
{
  stat.multi_scan_stat_.call_cnt_++;
  stat.multi_scan_stat_.output_row_cnt_ += table_stat_.output_row_cnt_;
}

int ObMultipleMultiScanMerge::construct_iters()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObITable*>& tables = tables_handle_.get_tables();

  consumer_.reset();

  if (OB_UNLIKELY(iters_.count() > 0 && iters_.count() != tables.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "iter cnt is not equal to table cnt",
        K(ret),
        "iter cnt",
        iters_.count(),
        "table cnt",
        tables.count(),
        KP(this));
  } else if (tables.count() > 0) {
    ObITable* table = NULL;
    ObStoreRowIterator* iter = NULL;
    const ObTableIterParam* iter_param = NULL;
    bool use_cache_iter = iters_.count() > 0;

    if (OB_FAIL(loser_tree_.init(tables.count(), *access_ctx_->stmt_allocator_))) {
      STORAGE_LOG(WARN, "init loser tree fail", K(ret));
    }

    for (int64_t i = tables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (OB_FAIL(tables.at(i, table))) {
        STORAGE_LOG(WARN, "Fail to get i store, ", K(i), K(ret));
      } else if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Fail to get access param", K(i), K(ret), K(*table));
      } else if (!use_cache_iter) {
        if (OB_FAIL(table->multi_scan(*iter_param, *access_ctx_, *ranges_, iter))) {
          STORAGE_LOG(WARN, "Fail to get iterator, ", K(ret), K(*iter_param), K(*access_ctx_), K(i));
        } else if (OB_FAIL(iters_.push_back(iter))) {
          iter->~ObStoreRowIterator();
          STORAGE_LOG(WARN, "Fail to push iter to iterator array, ", K(ret), K(i));
        }
      } else if (OB_ISNULL(iters_.at(tables.count() - 1 - i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null iter", K(ret), "idx", tables.count() - 1 - i, K_(iters));
      } else if (OB_FAIL(iter->init(*iter_param, *access_ctx_, table, ranges_))) {
        STORAGE_LOG(WARN, "failed to init scan iter", K(ret), "idx", tables.count() - i);
      }

      if (OB_SUCC(ret)) {
        consumer_.add_consumer(i);
        STORAGE_LOG(DEBUG, "add iter for consumer", KPC(table), KPC(access_param_));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(prepare_range_skip())) {
        STORAGE_LOG(WARN, "Fail to prepare range skip", K(ret));
      }
    }
  }

  return ret;
}

void ObMultipleMultiScanMerge::reuse()
{
  ObMultipleScanMergeImpl::reuse();
}

int ObMultipleMultiScanMerge::supply_consume()
{
  int ret = OB_SUCCESS;
  ObScanMergeLoserTreeItem item;
  const int64_t consumer_cnt = consumer_.get_consumer_num();
  for (int64_t i = 0; OB_SUCC(ret) && i < consumer_cnt; ++i) {
    const int64_t iter_idx = consumer_.get_consumer_iters()[i];
    ObStoreRowIterator* iter = iters_.at(iter_idx);
    if (NULL == iter) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error", K(ret), K(iter));
    } else if (OB_FAIL(iter->get_next_row_ext(item.row_, item.iter_flag_))) {
      if (common::OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "failed to get next row from iterator", "index", iter_idx, "iterator", *iter);
      } else if (try_push_top_item_ && 1 != consumer_cnt) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "try_push_top_item_ mismatch", K(ret), K(consumer_cnt));
      } else {
        ret = common::OB_SUCCESS;
      }
    } else if (OB_ISNULL(item.row_)) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get next row return NULL row", "iter_index", iter_idx, K(ret));
    } else {
      const int64_t iter_idx = consumer_.get_consumer_iters()[i];
      if (item.row_->is_get_) {
        get_items_[iter_idx] = item.row_;
        ++get_num_;
      } else {
        item.iter_idx_ = consumer_.get_consumer_iters()[i];
        if (try_push_top_item_) {
          if (1 != consumer_cnt) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "try_push_top_item_ mismatch", K(ret), K(consumer_cnt));
          } else if (OB_FAIL(loser_tree_.push_top(item))) {
            STORAGE_LOG(WARN, "loser tree push top error", K(ret));
          }
          try_push_top_item_ = false;
        } else if (OB_FAIL(loser_tree_.push(item))) {
          STORAGE_LOG(WARN, "loser tree push error", K(ret));
        } else {
          STORAGE_LOG(DEBUG, "push", K(item.iter_idx_), K(*item.row_), K(access_param_->iter_param_.table_id_));
        }
      }
      if (OB_SUCC(ret)) {
        if (0 == iter_idx) {
          ++row_stat_.inc_row_count_;
        } else {
          ++row_stat_.base_row_count_;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    // no worry, if no new items pushed, the rebuild will quickly exit
    if (OB_FAIL(loser_tree_.rebuild())) {
      STORAGE_LOG(WARN, "loser tree rebuild fail", K(ret));
    } else {
      consumer_.set_consumer_num(0);
      try_push_top_item_ = false;
    }
  }
  return ret;
}

int ObMultipleMultiScanMerge::inner_get_next_row_for_get(ObStoreRow& row, bool& need_retry)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* tmp_row = NULL;
  bool final_result = false;
  nop_pos_.reset();
  row.row_val_.count_ = 0;
  row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  row.from_base_ = false;
  need_retry = false;
  if (get_num_ < iters_.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get_num must not less than iters count", K(ret), K_(get_num), K(iters_.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
    tmp_row = get_items_[i];
    if (OB_ISNULL(tmp_row)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, row is NULL", K(ret));
    } else {
      --get_num_;
      if (!final_result) {
        row.scan_index_ = tmp_row->scan_index_;
        if (OB_FAIL(ObRowFuse::fuse_row(*tmp_row, row, nop_pos_, final_result))) {
          STORAGE_LOG(WARN, "fail to merge rows", K(ret), "tmp_row", *tmp_row, K(row));
        }
      }
    }
  }

  if (OB_SUCC(ret) && ObActionFlag::OP_ROW_EXIST != row.flag_) {
    need_retry = true;
  }

  if (OB_SUCC(ret)) {
    consumer_.set_consumer_num(0);
    get_num_ = 0;
    for (int64_t i = 0; i < iters_.count(); ++i) {
      consumer_.add_consumer(i);
    }
  }
  return ret;
}

int ObMultipleMultiScanMerge::inner_get_next_row_for_scan(ObStoreRow& row, bool& need_retry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMultipleScanMergeImpl::inner_get_next_row(row, need_retry))) {
    STORAGE_LOG(WARN, "fail to inner get next row for scan", K(ret));
  }
  STORAGE_LOG(DEBUG, "get_next_row", K(ret), K(row), K(need_retry));
  return ret;
}

int ObMultipleMultiScanMerge::inner_get_next_row(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  bool need_retry = true;
  bool is_data_ready = false;
  STORAGE_LOG(DEBUG, "inner_get_next_row", K(get_num_));
  if (OB_UNLIKELY(0 == iters_.count())) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret) && need_retry) {
      if (OB_FAIL(supply_consume())) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "supply consume failed", K(ret));
        }
      } else {
        if (0 == get_num_ && is_scan_end()) {
          ret = OB_ITER_END;
          STORAGE_LOG(DEBUG, "inner_get_next_row reach end");
          break;
        }
        if (OB_FAIL(is_scan_data_ready(is_data_ready))) {
          STORAGE_LOG(WARN, "fail to check if scan data is ready", K(ret));
        } else if (is_data_ready) {
          if (OB_FAIL(inner_get_next_row_for_scan(row, need_retry))) {
            STORAGE_LOG(WARN, "fail to get next row for scan", K(ret));
          }
        } else if (OB_FAIL(is_get_data_ready(is_data_ready))) {
          STORAGE_LOG(WARN, "fail t o check if get data is ready", K(ret));
        } else if (is_data_ready) {
          if (OB_FAIL(inner_get_next_row_for_get(row, need_retry))) {
            STORAGE_LOG(WARN, "fail to get next row for get", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected error, both get and scan have no data", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    row.range_array_idx_ = ranges_->at(row.scan_index_).get_range_array_idx();
    STORAGE_LOG(DEBUG, "multi_scan_merge: get_next_row", K(row));
  }
  return ret;
}

int ObMultipleMultiScanMerge::to_collation_free_range_on_demand(
    const ObIArray<ObExtStoreRange>& ranges, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (0 == ranges.count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid rowkeys count", K(ret), K(ranges.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    if (OB_FAIL(
            const_cast<ObExtStoreRange&>(ranges.at(i)).to_collation_free_range_on_demand_and_cutoff_range(allocator))) {
      STORAGE_LOG(WARN, "fail to get collation free rowkey", K(ret), K(ranges.at(i).get_range()));
    }
  }
  return ret;
}

int ObMultipleMultiScanMerge::estimate_row_count(const ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObIArray<common::ObExtStoreRange>& ranges, const common::ObIArray<ObITable*>& tables,
    ObPartitionEst& part_estimate, common::ObIArray<common::ObEstRowCountRecord>& est_records)
{
  int ret = OB_SUCCESS;

  part_estimate.reset();
  est_records.reuse();
  if (OB_INVALID_ID == table_id || ranges.count() <= 0 || tables.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(table_id), K(ranges), K(tables.count()));
  } else {
    // factors that including column cnt, column compare and primary key which is not included in output columns
    // are not calculated in the cost
    ObPartitionEst table_est;
    ObEstRowCountRecord record;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      int64_t start_time = common::ObTimeUtility::current_time();
      table_est.reset();
      ObITable* table = tables.at(i);
      if (NULL == table) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected error, store shouldn't be null", K(ret), KP(table));
      } else if (OB_SUCCESS != (ret = table->estimate_multi_scan_row_count(query_flag, table_id, ranges, table_est))) {
        STORAGE_LOG(WARN, "failed to estimate cost", K(ranges), K(table->get_key()), K(i), K(ret));
      } else if (OB_FAIL(part_estimate.add(table_est))) {
        STORAGE_LOG(WARN, "failed to add table estimation", K(ret), K(i), K(table_est), K(table->get_key()));
      } else {
        record.table_id_ = table->get_key().table_id_;
        record.table_type_ = table->get_key().table_type_;
        record.version_range_ = table->get_key().trans_version_range_;
        record.logical_row_count_ = table_est.logical_row_count_;
        record.physical_row_count_ = table_est.physical_row_count_;
        if (OB_FAIL(est_records.push_back(record))) {
          STORAGE_LOG(WARN, "failed to push back est row count record", K(ret));
        } else {
          STORAGE_LOG(DEBUG,
              "table estimate row count",
              K(table->get_key()),
              K(table_est),
              "cost time",
              common::ObTimeUtility::current_time() - start_time);
        }
      }
    }

    part_estimate.logical_row_count_ = part_estimate.logical_row_count_ < 0 ? 1 : part_estimate.logical_row_count_;
    part_estimate.physical_row_count_ = part_estimate.physical_row_count_ < 0 ? 1 : part_estimate.physical_row_count_;
    if (part_estimate.logical_row_count_ > part_estimate.physical_row_count_) {
      part_estimate.logical_row_count_ = part_estimate.physical_row_count_;
    }
    STORAGE_LOG(DEBUG, "final estimate", K(part_estimate));
  }
  return ret;
}

int ObMultipleMultiScanMerge::skip_to_range(const int64_t range_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(range_idx));
  } else if (range_idx >= ranges_->count()) {
    ret = OB_ITER_END;
  } else {
    // skip data in get items
    for (int64_t i = 0; i < iters_.count(); ++i) {
      if (nullptr != get_items_[i]) {
        if (get_items_[i]->scan_index_ < range_idx) {
          get_items_[i] = nullptr;
        }
      }
    }
    // skip data in heap and iterators
    const bool include_gap_key = ranges_->at(range_idx).get_range().get_border_flag().inclusive_start();
    const ObStoreRowkey& rowkey = ranges_->at(range_idx).get_range().get_start_key();
    STORAGE_LOG(DEBUG, "skip to range", K(include_gap_key), K(rowkey), K(range_idx));
    if (OB_FAIL(reset_range(0L /*skip all tables*/, range_idx, &rowkey, include_gap_key))) {
      STORAGE_LOG(WARN, "fail to reset range", K(ret));
    } else {
      try_push_top_item_ = false;
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
#endif
