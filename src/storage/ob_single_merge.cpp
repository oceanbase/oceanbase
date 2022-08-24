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

#include "ob_single_merge.h"
#include "blocksstable/ob_storage_cache_suite.h"
#include "blocksstable/ob_micro_block_reader.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
namespace storage {

ObSingleMerge::ObSingleMerge() : rowkey_(NULL), fuse_row_cache_fetcher_()
{}

ObSingleMerge::~ObSingleMerge()
{
  reset();
}

int ObSingleMerge::open(const ObExtStoreRowkey& rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMultipleMerge::open())) {
    STORAGE_LOG(WARN, "Fail to open ObMultipleMerge, ", K(ret));
  } else if (OB_FAIL(const_cast<ObExtStoreRowkey&>(rowkey).to_collation_free_on_demand_and_cutoff_range(
                 *access_ctx_->allocator_))) {
    STORAGE_LOG(WARN, "fail to get collation free rowkey", K(ret));
  } else if (OB_FAIL(fuse_row_cache_fetcher_.init(*access_param_, *access_ctx_))) {
    STORAGE_LOG(WARN, "fail to init fuse row cache fetcher", K(ret));
  } else {
    rowkey_ = &rowkey;
    row_filter_ = NULL;

    if (OB_UNLIKELY(access_ctx_->need_prewarm())) {
      access_ctx_->store_ctx_->warm_up_ctx_->record_get(*access_param_, *access_ctx_, rowkey);
    }
  }
  return ret;
}

void ObSingleMerge::reset()
{
  ObMultipleMerge::reset();
  rowkey_ = NULL;
  handle_.reset();
}

void ObSingleMerge::reuse()
{
  ObMultipleMerge::reuse();
  rowkey_ = NULL;
  handle_.reset();
}

int ObSingleMerge::calc_scan_range()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObSingleMerge::construct_iters()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObSingleMerge::is_range_valid() const
{
  return OB_SUCCESS;
}

int ObSingleMerge::get_table_row(const int64_t table_idx, const ObIArray<ObITable*>& tables, const ObStoreRow*& prow,
    ObStoreRow& fuse_row, bool& final_result, int64_t& sstable_end_log_ts)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* iter = NULL;
  const ObTableIterParam* iter_param = nullptr;
  ObITable* table = nullptr;
  prow = nullptr;
  if (table_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(table));
  } else if (OB_FAIL(tables.at(table_idx, table))) {
    STORAGE_LOG(WARN, "fail to get table", K(ret));
  } else if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Fail to get access param", K(table_idx), K(ret), K(*table));
  } else if (iters_.count() < tables.count() - table_idx) {
    // this table has not been accessed before
    if (OB_FAIL(table->get(*iter_param, *access_ctx_, *rowkey_, iter))) {
      STORAGE_LOG(WARN, "Fail to get row, ", K(ret), K(table_idx), K(iters_.count()), K(tables.count()));
    } else if (OB_FAIL(iters_.push_back(iter))) {
      iter->~ObStoreRowIterator();
      STORAGE_LOG(
          WARN, "Fail to push iter to iterator array, ", K(ret), K(table_idx), K(iters_.count()), K(tables.count()));
    }
  } else {
    iter = iters_.at(tables.count() - table_idx - 1);
    if (OB_FAIL(iter->init(*iter_param, *access_ctx_, table, rowkey_))) {
      STORAGE_LOG(WARN, "failed to init get iter", K(ret), K(table_idx), K(iters_.count()), K(tables.count()));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(iter->get_next_row(prow))) {
      STORAGE_LOG(WARN, "Fail to get row, ", K(ret), K(table_idx));
    } else if (OB_ISNULL(prow)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected error, the prow is NULL, ", K(ret));
    } else if (OB_FAIL(ObRowFuse::fuse_row(*prow, fuse_row, nop_pos_, final_result))) {
      STORAGE_LOG(WARN, "failed to merge rows", K(*prow), K(fuse_row), K(ret));
    } else {
      fuse_row.scan_index_ = 0;
      fuse_row.range_array_idx_ = 0;
      if (ObActionFlag::OP_ROW_DOES_NOT_EXIST != prow->flag_) {
        fuse_row.snapshot_version_ = std::max(fuse_row.snapshot_version_, prow->snapshot_version_);
        if (table->is_minor_sstable() && sstable_end_log_ts < table->get_end_log_ts()) {
          sstable_end_log_ts = table->get_end_log_ts();
        }
      }
      STORAGE_LOG(
          DEBUG, "process row fuse", K(*prow), K(fuse_row), K(access_ctx_->store_ctx_->mem_ctx_->get_read_snapshot()));
    }
  }
  return ret;
}

int ObSingleMerge::inner_get_next_row(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (NULL != rowkey_) {
    bool found_row_cache = false;
    bool has_frozen_memtable = false;
    int64_t end_table_idx = 0;
    int64_t row_cache_snapshot_version = 0;
    const ObIArray<ObITable*>& tables = tables_handle_.get_tables();
    const bool enable_fuse_row_cache = is_x86() && access_ctx_->use_fuse_row_cache_ &&
                                       access_param_->iter_param_.enable_fuse_row_cache() &&
                                       access_ctx_->fuse_row_cache_hit_rate_ > 6;
    access_ctx_->query_flag_.set_not_use_row_cache();
    const int64_t table_cnt = tables.count();
    ObITable* table = NULL;
    const ObStoreRow* prow = NULL;
    bool final_result = false;
    bool is_fuse_row_empty = false;
    int64_t sstable_end_log_ts = 0;
    ObStoreRow &fuse_row = full_row_;
    int64_t column_cnt = access_param_->iter_param_.projector_ != nullptr
                             ? access_param_->iter_param_.projector_->count()
                             : access_param_->iter_param_.out_cols_->count();
    nop_pos_.reset();
    fuse_row.row_val_.count_ = 0;
    fuse_row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
    fuse_row.from_base_ = false;
    fuse_row.snapshot_version_ = 0L;
    access_ctx_->use_fuse_row_cache_ = enable_fuse_row_cache;

    STORAGE_LOG(DEBUG,
        "single merge start to get next row",
        K(*rowkey_),
        K(access_ctx_->use_fuse_row_cache_),
        K(access_param_->iter_param_.enable_fuse_row_cache()));

    // firstly, try get from fuse row cache if memtable row is not final result
    if (OB_SUCC(ret) && enable_fuse_row_cache && !has_frozen_memtable) {
      if (OB_FAIL(fuse_row_cache_fetcher_.get_fuse_row_cache(rowkey_->get_store_rowkey(), handle_))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "fail to get from fuse row cache", K(ret), K(*rowkey_));
        } else {
          ++access_ctx_->access_stat_.fuse_row_cache_miss_cnt_;
          ++table_stat_.fuse_row_cache_miss_cnt_;
          ret = OB_SUCCESS;
        }
      } else {
        ++access_ctx_->access_stat_.fuse_row_cache_hit_cnt_;
        ++table_stat_.fuse_row_cache_hit_cnt_;
        row_cache_snapshot_version = handle_.value_->get_snapshot_version();
        sstable_end_log_ts = handle_.value_->get_sstable_end_log_ts();
        for (int64_t i = 0; OB_SUCC(ret) && i < table_cnt; ++i) {
          if (OB_FAIL(tables.at(i, table))) {
            STORAGE_LOG(WARN, "fail to get ith table", K(ret));
          } else {
            if (table->get_base_version() < row_cache_snapshot_version
                && row_cache_snapshot_version <= table->get_upper_trans_version()) {
              if (table->get_multi_version_start() >= row_cache_snapshot_version) {
                // do not use fuse row cache
                handle_.reset();
              } else {
                found_row_cache = true;
                end_table_idx = i;
                STORAGE_LOG(DEBUG, "fuse row cache info", K(*(handle_.value_)), K(sstable_end_log_ts), K(*table));
              }
              break;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (found_row_cache && end_table_idx == 0) {
            handle_.reset();
            found_row_cache = false;
          }
        }
      }
    }

    // secondly, try to get from other delta table
    for (int64_t i = table_cnt - 1; OB_SUCC(ret) && !final_result && i >= end_table_idx; --i) {
      if (OB_FAIL(get_table_row(i, tables, prow, fuse_row, final_result, sstable_end_log_ts))) {
        STORAGE_LOG(WARN, "fail to get table row", K(ret));
      }
    }

    STORAGE_LOG(DEBUG, "single merge middle to get next row", K(*rowkey_), K(fuse_row));

    if (OB_SUCC(ret) && !final_result && enable_fuse_row_cache) {
      if (!found_row_cache) {
        // row does not exist in both tables and row cache
      } else {
        ObStoreRow cache_row;
        is_fuse_row_empty = fuse_row.snapshot_version_ == 0;
        cache_row.row_val_.assign(handle_.value_->get_obj_ptr(), handle_.value_->get_obj_cnt());
        cache_row.flag_ = handle_.value_->get_flag();
        // cache_row.snapshot_version_ = handle_.value_->get_snapshot_version();
        if (is_fuse_row_empty) {
          row.row_val_.count_ = ObActionFlag::OP_ROW_EXIST == cache_row.flag_ ? column_cnt : 0;
          row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
          row.from_base_ = false;
          row.snapshot_version_ = 0L;
          if (OB_FAIL(project_row(cache_row, access_param_->iter_param_.projector_, 0 /*range idx delta*/, row))) {
            STORAGE_LOG(WARN, "fail to project row", K(ret));
          } else {
            final_result = true;
            STORAGE_LOG(DEBUG, "project fuse row cache", K(cache_row), K(row));
          }
        } else {
          if (OB_FAIL(ObRowFuse::fuse_row(cache_row, fuse_row, nop_pos_, final_result))) {
            STORAGE_LOG(WARN, "fail to fuse row", K(ret));
          } else {
            STORAGE_LOG(DEBUG, "fuse row cache", K(cache_row), K(fuse_row));
          }
        }
      }
    }

    if (OB_SUCC(ret) && enable_fuse_row_cache &&
        access_ctx_->enable_put_fuse_row_cache(SINGLE_GET_FUSE_ROW_CACHE_PUT_COUNT_THRESHOLD)) {
      // try to put row cache
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = fuse_row_cache_fetcher_.put_fuse_row_cache(
                             rowkey_->get_store_rowkey(), sstable_end_log_ts, fuse_row, handle_))) {
        STORAGE_LOG(WARN, "fail to put fuse row cache", K(ret));
      } else {
        access_ctx_->access_stat_.fuse_row_cache_put_cnt_++;
        table_stat_.fuse_row_cache_put_cnt_++;
      }
    }

    if (OB_SUCC(ret)) {
      STORAGE_LOG(DEBUG, "row before project", K(fuse_row));
      if (!is_fuse_row_empty) {
        row.row_val_.count_ = ObActionFlag::OP_ROW_EXIST == fuse_row.flag_ ? column_cnt : 0;
      }
      if (enable_fuse_row_cache) {
        if (!is_fuse_row_empty) {
          if (ObActionFlag::OP_ROW_EXIST == fuse_row.flag_) {
            if (OB_FAIL(project_row(fuse_row, access_param_->iter_param_.projector_, 0 /*range idx delta*/, row))) {
              STORAGE_LOG(WARN, "fail to project row", K(ret), K(fuse_row), KPC(access_param_->iter_param_.projector_));
            } else {
              STORAGE_LOG(DEBUG,
                  "after project row",
                  K(fuse_row),
                  K(row),
                  KPC(access_param_->iter_param_.projector_),
                  K(access_param_->iter_param_.table_id_));
            }
          } else {
            row.flag_ = fuse_row.flag_;
          }
        }
      } else {
        if (ObActionFlag::OP_ROW_EXIST == fuse_row.flag_) {
          if (OB_FAIL(project_row(fuse_row, nullptr, 0 /*range idx delta*/, row))) {
            STORAGE_LOG(WARN, "fail to project row", K(ret));
          }
        } else {
          row.flag_ = fuse_row.flag_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (row.flag_ != ObActionFlag::OP_ROW_EXIST) {
        ret = OB_ITER_END;
      }
      row.range_array_idx_ = rowkey_->get_range_array_idx();
      STORAGE_LOG(DEBUG, "get row", K(row));
    }
    rowkey_ = NULL;
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

void ObSingleMerge::collect_merge_stat(ObTableStoreStat& stat) const
{
  stat.single_get_stat_.call_cnt_++;
  stat.single_get_stat_.output_row_cnt_ += table_stat_.output_row_cnt_;
}

int ObSingleMerge::estimate_row_count(const ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObExtStoreRowkey& rowkey, const common::ObIArray<ObITable*>& stores, ObPartitionEst& part_estimate)
{
  int ret = OB_SUCCESS;
  UNUSEDx(query_flag, rowkey);
  if (OB_INVALID_ID == table_id || stores.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(table_id), K(ret), K(rowkey), K(stores.count()));
  } else {
    part_estimate.logical_row_count_ = part_estimate.physical_row_count_ = 1;
  }
  return ret;
}
}  // end namespace storage
}  // end namespace oceanbase
