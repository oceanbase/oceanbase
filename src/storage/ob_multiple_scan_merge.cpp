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

#include "storage/ob_multiple_scan_merge.h"
#include <math.h>
#include "storage/ob_row_fuse.h"

namespace oceanbase {
using namespace common;
namespace storage {

ObMultipleScanMerge::ObMultipleScanMerge() : ObMultipleScanMergeImpl(), range_(NULL), cow_range_()
{}

ObMultipleScanMerge::~ObMultipleScanMerge()
{}

int ObMultipleScanMerge::open(const ObExtStoreRange& range)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!range.get_range().is_valid())) {
    STORAGE_LOG(WARN, "Invalid range, ", K(range), K(ret));
  } else if (OB_FAIL(ObMultipleMerge::open())) {
    STORAGE_LOG(WARN, "Fail to open ObMultipleMerge, ", K(ret));
  } else if (OB_FAIL(const_cast<ObExtStoreRange&>(range).to_collation_free_range_on_demand_and_cutoff_range(
                 *access_ctx_->allocator_))) {
    STORAGE_LOG(WARN, "fail to get collation free rowkey", K(ret));
  } else {
    row_filter_ = NULL;

    if (NULL != access_ctx_->row_filter_ && tables_handle_.has_split_source_table(access_ctx_->pkey_)) {
      row_filter_ = access_ctx_->row_filter_;
    }

    range_ = &range;
    if (OB_FAIL(ObMultipleScanMergeImpl::prepare_loser_tree())) {
      STORAGE_LOG(WARN, "fail to prepare loser tree", K(ret));
    } else if (OB_FAIL(construct_iters())) {
      STORAGE_LOG(WARN, "fail to construct iters", K(ret));
    } else if (OB_UNLIKELY(access_ctx_->need_prewarm())) {
      access_ctx_->store_ctx_->warm_up_ctx_->record_scan(*access_param_, *access_ctx_, *range_);
    }
  }

  return ret;
}

int ObMultipleScanMerge::calc_scan_range()
{
  int ret = OB_SUCCESS;

  if (!curr_rowkey_.is_valid()) {
    // no row has been iterated
  } else if (NULL == access_ctx_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multiple scan merge not inited", K(ret));
  } else {
    if (range_ != &cow_range_) {
      cow_range_ = *range_;
      range_ = &cow_range_;
    }
    cow_range_.change_boundary(curr_rowkey_, access_ctx_->query_flag_.is_reverse_scan(), true);
    if (OB_FAIL(const_cast<ObExtStoreRange&>(cow_range_)
                    .to_collation_free_range_on_demand_and_cutoff_range(*access_ctx_->allocator_))) {
      STORAGE_LOG(WARN, "fail to get collation free rowkey", K(ret));
    }
  }

  return ret;
}

int ObMultipleScanMerge::is_range_valid() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(range_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "range is null", K(ret));
  } else if (!range_->get_range().is_valid()) {
    ret = OB_ITER_END;
  }
  return ret;
}

OB_INLINE int ObMultipleScanMerge::prepare()
{
  return ObMultipleScanMergeImpl::prepare_loser_tree();
}

void ObMultipleScanMerge::collect_merge_stat(ObTableStoreStat& stat) const
{
  stat.single_scan_stat_.call_cnt_++;
  stat.single_scan_stat_.output_row_cnt_ += table_stat_.output_row_cnt_;
}

int ObMultipleScanMerge::construct_iters()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObITable*>& tables = tables_handle_.get_tables();

  consumer_.reset();

  if (OB_ISNULL(range_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "range is NULL", K(ret));
  } else if (OB_UNLIKELY(iters_.count() > 0 && iters_.count() != tables.count())) {
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
    const ObTableIterParam* iter_pram = NULL;
    const bool use_cache_iter = iters_.count() > 0;
    const int64_t table_cnt = tables.count() - 1;

    if (OB_FAIL(loser_tree_.init(tables.count(), *access_ctx_->stmt_allocator_))) {
      STORAGE_LOG(WARN, "init loser tree fail", K(ret));
    }

    for (int64_t i = table_cnt; OB_SUCC(ret) && i >= 0; --i) {
      if (OB_FAIL(tables.at(i, table))) {
        STORAGE_LOG(WARN, "Fail to get ith store, ", K(i), K(ret));
      } else if (OB_ISNULL(iter_pram = get_actual_iter_param(table))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Fail to get access param", K(i), K(ret), K(*table));
      } else if (!use_cache_iter) {
        if (OB_FAIL(table->scan(*iter_pram, *access_ctx_, *range_, iter))) {
          STORAGE_LOG(WARN, "Fail to get iterator, ", K(*iter_pram), K(*access_ctx_), K(ret), K(i));
        } else if (OB_FAIL(iters_.push_back(iter))) {
          iter->~ObStoreRowIterator();
          STORAGE_LOG(WARN, "Fail to push iter to iterator array, ", K(ret), K(i));
        }
      } else if (OB_ISNULL(iter = iters_.at(table_cnt - i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null iter", K(ret), "idx", table_cnt - i, K_(iters));
      } else if (OB_FAIL(iter->init(*iter_pram, *access_ctx_, table, range_))) {
        STORAGE_LOG(WARN, "failed to init scan iter", K(ret), "idx", table_cnt - i);
      }

      if (OB_SUCC(ret)) {
        consumer_.add_consumer(i);
        if (iter->is_base_sstable_iter()) {
          consumer_.set_base_iter_idx(table_cnt - i);
        }
        STORAGE_LOG(DEBUG, "[PUSHDOWN]", K_(consumer), K(iter->is_base_sstable_iter()));
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

void ObMultipleScanMerge::reset()
{
  ObMultipleScanMergeImpl::reset();
  range_ = NULL;
  cow_range_.reset();
}

void ObMultipleScanMerge::reuse()
{
  return ObMultipleScanMergeImpl::reuse();
}

int ObMultipleScanMerge::inner_get_next_row(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ObMultipleScanMergeImpl::inner_get_next_row(row))) {
    row.range_array_idx_ = range_->get_range_array_idx();
    STORAGE_LOG(DEBUG, "get next row", K(row));
  }
  return ret;
}

int ObMultipleScanMerge::estimate_row_count(const ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObExtStoreRange& range, const common::ObIArray<ObITable*>& tables, ObPartitionEst& part_estimate,
    common::ObIArray<common::ObEstRowCountRecord>& est_records)
{
  int ret = OB_SUCCESS;
  part_estimate.reset();
  est_records.reuse();
  if (OB_UNLIKELY(OB_INVALID_ID == table_id || tables.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(table_id), K(range), K(tables.count()));
  } else if (OB_UNLIKELY(range.get_range().empty())) {
    // empty range
  } else {
    ObPartitionEst table_est;
    ObEstRowCountRecord record;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      int64_t start_time = common::ObTimeUtility::current_time();
      ObITable* table = tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected error, table shouldn't be null", K(ret), KP(table));
      } else if (OB_SUCCESS != (ret = table->estimate_scan_row_count(query_flag, table_id, range, table_est))) {
        STORAGE_LOG(WARN, "failed to estimate cost", K(range), K(i), K(ret), K(table->get_key()));
      } else if (OB_FAIL(part_estimate.add(table_est))) {
        STORAGE_LOG(WARN, "failed to add partition est", K(ret));
      } else {
        record.table_id_ = table->get_key().table_id_;
        record.table_type_ = table->get_key().table_type_;
        record.version_range_ = table->get_key().trans_version_range_;
        record.logical_row_count_ = table_est.logical_row_count_;
        record.physical_row_count_ = table_est.physical_row_count_;
        if (OB_FAIL(est_records.push_back(record))) {
          STORAGE_LOG(WARN, "failed to push back est row count record", K(ret));
        } else {
          STORAGE_LOG(TRACE,
              "table estimate row count",
              K(table->get_key()),
              K(table_est),
              K(range),
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
    STORAGE_LOG(DEBUG, "final estimate", K(part_estimate), K(table_id), K(range));
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
