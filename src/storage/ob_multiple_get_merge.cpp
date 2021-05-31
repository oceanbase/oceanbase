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

#include "storage/ob_multiple_get_merge.h"
#include "storage/ob_row_fuse.h"

namespace oceanbase {
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
namespace storage {
const int64_t ObMultipleGetMerge::MAX_MULTI_GET_FUSE_ROW_CACHE_PUT_COUNT = 50;

ObMultipleGetMerge::ObMultipleGetMerge()
    : rowkeys_(NULL),
      cow_rowkeys_(),
      prefetch_range_idx_(0),
      get_row_range_idx_(0),
      prefetch_cnt_(0),
      rows_(nullptr),
      handles_(nullptr),
      fuse_row_cache_fetcher_(),
      has_frozen_memtable_(false),
      can_prefetch_all_(false),
      end_memtable_idx_(0),
      sstable_begin_iter_idx_(0)
{}

ObMultipleGetMerge::~ObMultipleGetMerge()
{
  reset();
}

int ObMultipleGetMerge::open(const common::ObIArray<common::ObExtStoreRowkey>& rowkeys)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rowkeys.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret));
  } else if (OB_FAIL(ObMultipleMerge::open())) {
    STORAGE_LOG(WARN, "Fail to open ObMultipleMerge, ", K(ret));
  } else if (OB_FAIL(to_collation_free_rowkey_on_demand(rowkeys, *access_ctx_->allocator_))) {
    STORAGE_LOG(WARN, "fail to get collation free rowkeys", K(ret));
  } else {
    rowkeys_ = &rowkeys;
    row_filter_ = NULL;
    if (OB_FAIL(construct_iters())) {
      STORAGE_LOG(WARN, "fail to construct iters", K(ret));
    } else if (OB_UNLIKELY(access_ctx_->need_prewarm())) {
      access_ctx_->store_ctx_->warm_up_ctx_->record_multi_get(*access_param_, *access_ctx_, *rowkeys_);
    }
  }

  return ret;
}

void ObMultipleGetMerge::reset_with_fuse_row_cache()
{
  prefetch_range_idx_ = 0;
  get_row_range_idx_ = 0;
  sstable_rowkeys_.reset();
  if (nullptr != rows_) {
    for (int64_t i = 0; i < prefetch_cnt_; ++i) {
      rows_[i].~ObQueryRowInfo();
    }
    rows_ = nullptr;
  }
  if (nullptr != handles_) {
    for (int64_t i = 0; i < prefetch_cnt_; ++i) {
      handles_[i].reset();
      handles_[i].~ObFuseRowValueHandle();
    }
    handles_ = nullptr;
  }
  prefetch_cnt_ = 0;
  reuse_iter_array();
}

void ObMultipleGetMerge::reset()
{
  ObMultipleMerge::reset();
  rowkeys_ = NULL;
  cow_rowkeys_.reset();
  reset_with_fuse_row_cache();
}

void ObMultipleGetMerge::reuse()
{
  ObMultipleMerge::reuse();
  reset_with_fuse_row_cache();
}

int ObMultipleGetMerge::prepare()
{
  reset_with_fuse_row_cache();
  return OB_SUCCESS;
}

int ObMultipleGetMerge::calc_scan_range()
{
  int ret = OB_SUCCESS;

  if (!curr_rowkey_.is_valid()) {
    // no row has been iterated
  } else if (OB_ISNULL(rowkeys_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rowkeys is NULL", K(ret));
  } else {
    GetRowkeyArray tmp_rowkeys;
    for (int64_t i = 0; i < rowkeys_->count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(tmp_rowkeys.push_back(rowkeys_->at(i)))) {
        STORAGE_LOG(WARN, "push back rowkey failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (rowkeys_ != &cow_rowkeys_) {
        cow_rowkeys_.reset();
        rowkeys_ = &cow_rowkeys_;
      }
    }

    if (OB_SUCC(ret)) {
      int64_t l = curr_scan_index_ + 1;
      int64_t r = tmp_rowkeys.count();

      cow_rowkeys_.reuse();
      range_idx_delta_ += curr_scan_index_ + 1;
      for (int64_t i = l; i < r && OB_SUCC(ret); ++i) {
        if (OB_FAIL(cow_rowkeys_.push_back(tmp_rowkeys.at(i)))) {
          STORAGE_LOG(WARN, "push back rowkey failed", K(ret));
        }
      }
      STORAGE_LOG(DEBUG, "skip rowkeys", K(cow_rowkeys_), K(range_idx_delta_));
    }
  }

  return ret;
}

int ObMultipleGetMerge::is_range_valid() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowkeys_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rowkeys is null", K(ret));
  } else if (0 == rowkeys_->count()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObMultipleGetMerge::construct_iters_with_fuse_row_cache()
{
  int ret = OB_SUCCESS;
  reset_with_fuse_row_cache();
  const ObIArray<ObITable*>& tables = tables_handle_.get_tables();
  ObStoreRowIterator* iter = NULL;
  const ObTableIterParam* iter_param = nullptr;
  const int64_t iter_cnt = iters_.count();
  int64_t iter_idx = 0;
  int64_t memtable_cnt = 0;
  if (access_ctx_->fuse_row_cache_hit_rate_ > 50) {
    access_ctx_->query_flag_.set_use_fuse_row_cache();
  } else {
    access_ctx_->query_flag_.set_not_use_fuse_row_cache();
  }
  const bool enable_fuse_row_cache = access_ctx_->use_fuse_row_cache_ &&
                                     access_param_->iter_param_.enable_fuse_row_cache() &&
                                     access_ctx_->query_flag_.is_use_fuse_row_cache();
  access_ctx_->use_fuse_row_cache_ = enable_fuse_row_cache;

  if (OB_FAIL(fuse_row_cache_fetcher_.init(*access_param_, *access_ctx_))) {
    STORAGE_LOG(WARN, "fail to init fuse row cache fetcher", K(ret));
  } else {
    access_ctx_->query_flag_.set_not_use_row_cache();
  }
  // construct memtable iterators
  end_memtable_idx_ = tables.count();
  for (int64_t i = tables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    ObITable* table = nullptr;
    if (OB_FAIL(tables.at(i, table))) {
      STORAGE_LOG(WARN, "fail to get table", K(ret));
    } else if (table->is_memtable()) {
      ++memtable_cnt;
      end_memtable_idx_ = i;
      if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to get iter param", K(ret), K(i), K(*table));
      } else if (iter_idx >= iter_cnt) {
        if (OB_FAIL(table->multi_get(*iter_param, *access_ctx_, *rowkeys_, iter))) {
          STORAGE_LOG(WARN, "fail to multi get", K(ret));
        } else if (OB_FAIL(iters_.push_back(iter))) {
          iter->~ObStoreRowIterator();
          iter = nullptr;
          STORAGE_LOG(WARN, "fail to push back iter", K(ret));
        }
      } else if (OB_FAIL(iters_.at(iter_idx)->init(*iter_param, *access_ctx_, table, rowkeys_))) {
        STORAGE_LOG(WARN, "fail to init iterator", K(ret));
      }
      ++iter_idx;
    } else {
      sstable_begin_iter_idx_ = memtable_cnt;
      break;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(alloc_resource())) {
      STORAGE_LOG(WARN, "fail to alloc resource", K(ret));
    } else if (OB_FAIL(construct_sstable_iter())) {
      STORAGE_LOG(WARN, "fail to construct sstable iterator", K(ret));
    }
  }
  return ret;
}

int ObMultipleGetMerge::construct_iters_without_fuse_row_cache()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObITable*>& tables = tables_handle_.get_tables();
  access_ctx_->fq_ctx_ = nullptr;

  if (iters_.count() > 0 && iters_.count() != tables.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "iter cnt is not equal to table cnt",
        K(ret),
        "iter cnt",
        iters_.count(),
        "table cnt",
        tables.count(),
        KP(this));
  } else {
    ObITable* table = NULL;
    ObStoreRowIterator* iter = NULL;
    const ObTableIterParam* iter_param = NULL;
    bool use_cache_iter = iters_.count() > 0;

    for (int64_t i = tables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (OB_FAIL(tables.at(i, table))) {
        STORAGE_LOG(WARN, "Fail to get ith store, ", K(i), K(ret));
      } else if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Fail to get access param", K(i), K(ret), K(*table));
      } else if (!use_cache_iter) {
        if (OB_FAIL(table->multi_get(*iter_param, *access_ctx_, *rowkeys_, iter))) {
          STORAGE_LOG(WARN, "Fail to get iterator, ", K(ret));
        } else if (OB_FAIL(iters_.push_back(iter))) {
          iter->~ObStoreRowIterator();
          STORAGE_LOG(WARN, "Fail to push iter to iterator array, ", K(ret));
        }
      } else if (OB_FAIL(iters_.at(tables.count() - 1 - i)->init(*iter_param, *access_ctx_, table, rowkeys_))) {
        STORAGE_LOG(WARN, "failed to init multi getter", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObMultipleGetMerge::construct_iters()
{
  int ret = OB_SUCCESS;
  can_prefetch_all_ = is_x86() && rowkeys_->count() <= MAX_PREFETCH_CNT;
  if (can_prefetch_all_) {
    if (OB_FAIL(construct_iters_with_fuse_row_cache())) {
      STORAGE_LOG(WARN, "fail to construct iters with fuse row cache", K(ret));
    }
  } else {
    access_ctx_->use_fuse_row_cache_ = false;
    if (OB_FAIL(construct_iters_without_fuse_row_cache())) {
      STORAGE_LOG(WARN, "fail to construct iters without fuse row cache", K(ret));
    }
  }
  return ret;
}

int ObMultipleGetMerge::try_get_fuse_row_cache(int64_t& end_table_idx)
{
  int ret = OB_SUCCESS;
  const int64_t handle_idx = prefetch_range_idx_ % prefetch_cnt_;
  rows_[handle_idx].state_ = ObMultiGetRowState::INVALID;
  end_table_idx = 0;
  if (!has_frozen_memtable_ && access_ctx_->use_fuse_row_cache_ &&
      access_ctx_->enable_get_fuse_row_cache(MAX_MULTI_GET_FUSE_ROW_CACHE_GET_COUNT)) {
    ObFuseRowValueHandle& handle = handles_[handle_idx];
    if (OB_FAIL(
            fuse_row_cache_fetcher_.get_fuse_row_cache(rowkeys_->at(prefetch_range_idx_).get_store_rowkey(), handle))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        STORAGE_LOG(WARN, "fail to get fuse row cache", K(ret));
      } else {
        ret = OB_SUCCESS;
        ++access_ctx_->access_stat_.fuse_row_cache_miss_cnt_;
        ++table_stat_.fuse_row_cache_miss_cnt_;
        STORAGE_LOG(DEBUG,
            "current rowkey not exist in fuse row cache",
            K(prefetch_range_idx_),
            K(rowkeys_->at(prefetch_range_idx_)));
      }
    } else {
      ++access_ctx_->access_stat_.fuse_row_cache_hit_cnt_;
      ++table_stat_.fuse_row_cache_hit_cnt_;
      ObIArray<ObITable*>& tables = tables_handle_.get_tables();
      ObITable* table = nullptr;
      const int64_t table_cnt = tables.count();
      const int64_t row_cache_snapshot_version = handle.value_->get_snapshot_version();
      const int64_t sstable_end_log_ts = handle.value_->get_sstable_end_log_ts();
      for (int64_t i = 0; OB_SUCC(ret) && i < table_cnt; ++i) {
        if (OB_FAIL(tables.at(i, table))) {
          STORAGE_LOG(WARN, "fail to get ith table", K(ret));
        } else {
          if (table->get_base_version() < row_cache_snapshot_version &&
              row_cache_snapshot_version < table->get_upper_trans_version() &&
              (!table->is_multi_version_minor_sstable() || sstable_end_log_ts <= table->get_end_log_ts())) {
            if (table->get_multi_version_start() >= row_cache_snapshot_version) {
              // do not use fuse row cache
              handle.reset();
            } else {
              access_ctx_->fq_ctx_ = handle.value_->get_fq_ctx();
              end_table_idx = i;
              rows_[handle_idx].state_ = ObMultiGetRowState::IN_FUSE_ROW_CACHE;
              rows_[handle_idx].sstable_end_log_ts_ = sstable_end_log_ts;
              STORAGE_LOG(DEBUG, "fuse row cache info", K(*(handle.value_)), K(end_table_idx), K(*table));
            }
            break;
          }
        }
      }
      STORAGE_LOG(DEBUG,
          "current rowkey exist in fuse row cache",
          K(prefetch_range_idx_),
          K(rowkeys_->at(prefetch_range_idx_)));
    }
  }
  return ret;
}

int ObMultipleGetMerge::try_put_fuse_row_cache(ObQueryRowInfo& row_info)
{
  int ret = OB_SUCCESS;
  int64_t put_threshold =
      (access_ctx_->fuse_row_cache_hit_rate_ - access_ctx_->block_cache_hit_rate_) * rowkeys_->count() / 100;
  put_threshold = std::max(1L, put_threshold);
  put_threshold = std::min(MAX_MULTI_GET_FUSE_ROW_CACHE_PUT_COUNT, put_threshold);
  if (access_ctx_->use_fuse_row_cache_ && access_ctx_->enable_put_fuse_row_cache(put_threshold) &&
      access_ctx_->fuse_row_cache_hit_rate_ > access_ctx_->block_cache_hit_rate_) {
    const int64_t handle_idx = get_row_range_idx_ % prefetch_cnt_;
    const ObStoreRowkey& rowkey = rowkeys_->at(get_row_range_idx_).get_store_rowkey();
    ObFuseRowValueHandle& handle = handles_[handle_idx];
    if (OB_FAIL(
            fuse_row_cache_fetcher_.put_fuse_row_cache(rowkey, row_info.sstable_end_log_ts_, row_info.row_, handle))) {
      STORAGE_LOG(WARN, "fail to put fuse row cache", K(ret));
    } else {
      access_ctx_->access_stat_.fuse_row_cache_put_cnt_++;
      table_stat_.fuse_row_cache_put_cnt_++;
    }
  }

  return ret;
}

int ObMultipleGetMerge::alloc_resource()
{
  int ret = OB_SUCCESS;
  prefetch_cnt_ = rowkeys_->count();
  if (nullptr == handles_) {
    void* buf = nullptr;
    if (OB_ISNULL(buf = access_ctx_->allocator_->alloc(sizeof(ObFuseRowValueHandle) * prefetch_cnt_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
    } else {
      handles_ = new (buf) ObFuseRowValueHandle[prefetch_cnt_];
    }
  }
  if (OB_SUCC(ret) && nullptr == rows_) {
    void* buf = nullptr;
    if (OB_ISNULL(buf = access_ctx_->allocator_->alloc(sizeof(ObQueryRowInfo) * prefetch_cnt_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
    } else {
      rows_ = new (buf) ObQueryRowInfo[prefetch_cnt_];
      int64_t max_cell_cnt = std::max(access_param_->get_max_out_col_cnt(), access_param_->out_col_desc_param_.count());
      max_cell_cnt = std::max(max_cell_cnt, access_param_->reserve_cell_cnt_);
      for (int64_t i = 0; OB_SUCC(ret) && i < prefetch_cnt_; ++i) {
        ObQueryRowInfo& row_info = rows_[i];
        if (OB_FAIL(alloc_row(*access_ctx_->allocator_, max_cell_cnt, row_info.row_))) {
          STORAGE_LOG(WARN, "fail to alloc row", K(ret));
        } else if (OB_FAIL(row_info.nop_pos_.init(*access_ctx_->allocator_, max_cell_cnt))) {
          STORAGE_LOG(WARN, "fail to init nop pos", K(ret));
        } else {
          row_info.row_.scan_index_ = -1;
        }
      }
    }
  }
  return ret;
}

int ObMultipleGetMerge::construct_sstable_iter()
{
  int ret = OB_SUCCESS;

  // try to get fuse row cache or memtable to identify need to touch sstable data
  const int64_t prefetch_cnt = rowkeys_->count();
  for (int64_t i = 0; OB_SUCC(ret) && i < prefetch_cnt; ++i) {
    if (OB_FAIL(prefetch())) {
      STORAGE_LOG(WARN, "fail to prefetch", K(ret));
    }
  }
  if (OB_SUCC(ret) && sstable_rowkeys_.count() > 0) {
    ObStoreRowIterator* iter = nullptr;
    const ObIArray<ObITable*>& tables = tables_handle_.get_tables();
    const ObTableIterParam* iter_param = nullptr;
    const int64_t iter_cnt = iters_.count();
    int64_t iter_idx = sstable_begin_iter_idx_;
    for (int64_t i = end_memtable_idx_ - 1; OB_SUCC(ret) && i >= 0; --i) {
      ObITable* table = nullptr;
      if (OB_FAIL(tables.at(i, table))) {
        STORAGE_LOG(WARN, "fail to get table", K(ret));
      } else if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to get iter param", K(ret), K(i), K(*table));
      } else if (iter_idx >= iter_cnt) {
        if (OB_FAIL(table->multi_get(*iter_param, *access_ctx_, sstable_rowkeys_, iter))) {
          STORAGE_LOG(WARN, "fail to multi get", K(ret));
        } else if (OB_FAIL(iters_.push_back(iter))) {
          iter->~ObStoreRowIterator();
          iter = nullptr;
          STORAGE_LOG(WARN, "fail to push back iter", K(ret));
        }
      } else if (OB_FAIL(iters_.at(iter_idx)->init(*iter_param, *access_ctx_, table, &sstable_rowkeys_))) {
        STORAGE_LOG(WARN, "fail to init iterator", K(ret));
      }
      ++iter_idx;
    }
  }
  return ret;
}

void ObMultipleGetMerge::reuse_row(const int64_t rowkey_idx, ObQueryRowInfo& row)
{
  row.nop_pos_.reset();
  row.row_.row_val_.count_ = 0;
  row.row_.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  row.row_.from_base_ = false;
  row.row_.snapshot_version_ = 0L;
  row.row_.scan_index_ = rowkey_idx;
  row.final_result_ = false;
  row.sstable_end_log_ts_ = 0;
}

int ObMultipleGetMerge::get_table_row(const int64_t table_idx, const int64_t rowkey_idx, bool& stop_reading)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator* iter = NULL;
  const ObStoreRow* prow = nullptr;
  ObQueryRowInfo& row_info = rows_[rowkey_idx % prefetch_cnt_];
  ObITable* table = tables_handle_.get_table(table_idx);
  if (table_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(table));
  } else if (OB_FAIL(iters_.at(tables_handle_.get_tables().count() - table_idx - 1, iter))) {
    STORAGE_LOG(WARN, "fail to get iterator", K(ret));
  } else if (OB_FAIL(iter->get_next_row(prow))) {
    STORAGE_LOG(WARN, "Fail to get row, ", K(ret), K(table_idx));
  } else if (OB_ISNULL(prow)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, the prow is NULL, ", K(ret));
  } else if (stop_reading || row_info.final_result_) {
  } else if (OB_FAIL(ObRowFuse::fuse_row(*prow, row_info.row_, row_info.nop_pos_, row_info.final_result_))) {
    STORAGE_LOG(WARN, "failed to merge rows", K(*prow), K(row_info), K(ret));
  } else {
    row_info.row_.scan_index_ = rowkey_idx;
    if (ObActionFlag::OP_ROW_DOES_NOT_EXIST != prow->flag_) {
      row_info.row_.snapshot_version_ = std::max(row_info.row_.snapshot_version_, prow->snapshot_version_);
      if (table->is_multi_version_minor_sstable() && row_info.sstable_end_log_ts_ < table->get_end_log_ts()) {
        row_info.sstable_end_log_ts_ = table->get_end_log_ts();
      }
    } else if (!prow->fq_ctx_.is_valid() && table->is_memtable()) {
      stop_reading = true;
    }
    row_info.row_.fq_ctx_ = prow->fq_ctx_.is_valid() ? prow->fq_ctx_ : row_info.row_.fq_ctx_;
    STORAGE_LOG(DEBUG,
        "process row fuse",
        KP(this),
        "row",
        prow->flag_ == ObActionFlag::OP_ROW_DOES_NOT_EXIST ? "not exist" : to_cstring(*prow),
        K(row_info.row_),
        K(row_info.row_.fq_ctx_),
        K(access_ctx_->store_ctx_->mem_ctx_->get_read_snapshot()),
        K(stop_reading));
  }
  return ret;
}

int ObMultipleGetMerge::prefetch()
{
  int ret = OB_SUCCESS;
  if (prefetch_cnt_ != rowkeys_->count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "error unexpected, prefetch cnt is not equal to rowkeys' count",
        K(ret),
        K(prefetch_cnt_),
        "rowkey_count",
        rowkeys_->count());
  } else if (prefetch_range_idx_ < rowkeys_->count()) {
    const int64_t handle_idx = prefetch_range_idx_ % prefetch_cnt_;
    ObQueryRowInfo& row_info = rows_[handle_idx];
    int64_t end_table_idx = 0;
    bool stop_reading = false;
    if (row_info.row_.scan_index_ != prefetch_range_idx_) {
      reuse_row(prefetch_range_idx_, row_info);
    }
    if (OB_FAIL(try_get_fuse_row_cache(end_table_idx))) {
      STORAGE_LOG(WARN, "fail to get from fuse row cache", K(ret));
    } else {
      ObIArray<ObITable*>& tables = tables_handle_.get_tables();
      const int64_t table_cnt = tables.count();
      if (ObMultiGetRowState::IN_FUSE_ROW_CACHE == row_info.state_) {
        ObITable* table = nullptr;
        // should call memtable iterator's get_next_row method to consume the row
        const int64_t end_get_idx = std::min(end_table_idx, end_memtable_idx_);
        for (int64_t i = table_cnt - 1; OB_SUCC(ret) && i >= end_get_idx; --i) {
          if (OB_FAIL(tables.at(i, table))) {
            STORAGE_LOG(WARN, "fail to get ith table", K(ret));
          } else if (table->is_memtable()) {
            if (OB_FAIL(get_table_row(i, prefetch_range_idx_, stop_reading))) {
              STORAGE_LOG(WARN, "fail to get table row", K(ret));
            } else if ((stop_reading || row_info.final_result_) && i == end_memtable_idx_) {
              STORAGE_LOG(DEBUG, "current row is final result", K(row_info.row_));
              break;
            }
          } else {
            row_info.state_ = ObMultiGetRowState::IN_FUSE_ROW_CACHE_AND_SSTABLE;
            row_info.end_iter_idx_ = table_cnt - 1 - end_table_idx;
            if (OB_FAIL(sstable_rowkeys_.push_back(rowkeys_->at(prefetch_range_idx_)))) {
              STORAGE_LOG(WARN, "fail to push back sstable rowkeys", K(ret));
            }
            STORAGE_LOG(DEBUG, "row in sstable", K(rowkeys_->at(prefetch_range_idx_)));
            break;
          }
        }
      } else {
        // try get from latest memtable
        access_ctx_->fq_ctx_ = nullptr;
        for (int64_t i = table_cnt - 1; OB_SUCC(ret) && i >= end_memtable_idx_; --i) {
          if (OB_FAIL(get_table_row(i, prefetch_range_idx_, stop_reading))) {
            STORAGE_LOG(WARN, "fail to get table row", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (row_info.final_result_) {
            row_info.state_ = ObMultiGetRowState::IN_MEMTABLE;
            STORAGE_LOG(DEBUG, "current row is in memtable and final result", K(rows_[handle_idx].row_), K(handle_idx));
          } else if (OB_FAIL(sstable_rowkeys_.push_back(rowkeys_->at(prefetch_range_idx_)))) {
            STORAGE_LOG(WARN, "fail to push back sstable rowkey", K(ret));
          } else {
            row_info.state_ = ObMultiGetRowState::IN_SSTABLE;
            STORAGE_LOG(DEBUG, "current row in sstable", K(rowkeys_->at(prefetch_range_idx_)));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ++prefetch_range_idx_;
      }
    }
  }

  return ret;
}

int ObMultipleGetMerge::inner_get_next_row_with_fuse_row_cache(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_UNLIKELY(0 == iters_.count())) {
      ret = OB_ITER_END;
    } else if (get_row_range_idx_ >= rowkeys_->count()) {
      ret = OB_ITER_END;
    } else {
      const int64_t row_idx = get_row_range_idx_ % prefetch_cnt_;
      ObQueryRowInfo& row_info = rows_[row_idx];
      ObFuseRowValueHandle& handle = handles_[row_idx];
      if (row_info.final_result_) {
        // final result, do nothing
      } else if (ObMultiGetRowState::IN_FUSE_ROW_CACHE == row_info.state_) {
        ObStoreRow cache_row;
        cache_row.row_val_.assign(handle.value_->get_obj_ptr(), handle.value_->get_obj_cnt());
        cache_row.flag_ = handle.value_->get_flag();
        if (OB_FAIL(ObRowFuse::fuse_row(cache_row, row_info.row_, row_info.nop_pos_, row_info.final_result_))) {
          STORAGE_LOG(WARN, "fail to fuse row", K(ret));
        }
        STORAGE_LOG(DEBUG, "fuse row cache row", K(cache_row), K(row_info.row_));
      } else if (ObMultiGetRowState::IN_FUSE_ROW_CACHE_AND_SSTABLE == row_info.state_) {
        const ObStoreRow* prow = nullptr;
        STORAGE_LOG(DEBUG, "in fuse row cache and sstable", K(sstable_begin_iter_idx_), K(row_info.end_iter_idx_));
        for (int64_t i = sstable_begin_iter_idx_; OB_SUCC(ret) && i < iters_.count(); ++i) {
          if (OB_FAIL(iters_[i]->get_next_row(prow))) {
            // iter end is not expected
            STORAGE_LOG(WARN, "fail to get next row", K(ret));
          } else if (i > row_info.end_iter_idx_) {
          } else if (!row_info.final_result_) {
            if (OB_FAIL(ObRowFuse::fuse_row(*prow, row_info.row_, row_info.nop_pos_, row_info.final_result_))) {
              STORAGE_LOG(WARN, "fail to fuse row", K(ret));
            } else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST != prow->flag_) {
              ObITable* table = tables_handle_.get_table(tables_handle_.get_count() - i - 1);
              row_info.row_.snapshot_version_ = std::max(row_info.row_.snapshot_version_, prow->snapshot_version_);
              if (table->is_multi_version_minor_sstable() && row_info.sstable_end_log_ts_ < table->get_end_log_ts()) {
                row_info.sstable_end_log_ts_ = table->get_end_log_ts();
              }
            }
          }
        }
        if (OB_SUCC(ret) && !row_info.final_result_) {
          ObStoreRow cache_row;
          cache_row.row_val_.assign(handle.value_->get_obj_ptr(), handle.value_->get_obj_cnt());
          cache_row.flag_ = handle.value_->get_flag();
          if (OB_FAIL(ObRowFuse::fuse_row(cache_row, row_info.row_, row_info.nop_pos_, row_info.final_result_))) {
            STORAGE_LOG(WARN, "fail to fuse row", K(ret));
          }
        }
      } else if (ObMultiGetRowState::IN_SSTABLE == row_info.state_ &&
                 end_memtable_idx_ > 0 /*have non-empty sstable*/) {
        const ObStoreRow* prow = nullptr;
        STORAGE_LOG(DEBUG, "row in sstable", K(sstable_begin_iter_idx_), K(iters_.count()));
        for (int64_t i = sstable_begin_iter_idx_; OB_SUCC(ret) && i < iters_.count(); ++i) {
          if (OB_FAIL(iters_[i]->get_next_row(prow))) {
            // iter end is not expected
            STORAGE_LOG(WARN, "fail to get next row", K(ret));
          } else if (!row_info.final_result_) {
            if (OB_FAIL(ObRowFuse::fuse_row(*prow, row_info.row_, row_info.nop_pos_, row_info.final_result_))) {
              STORAGE_LOG(WARN, "fail to fuse row", K(ret));
            } else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST != prow->flag_) {
              ObITable* table = tables_handle_.get_table(tables_handle_.get_count() - i - 1);
              row_info.row_.snapshot_version_ = std::max(row_info.row_.snapshot_version_, prow->snapshot_version_);
              if (row_info.sstable_end_log_ts_ < table->get_end_log_ts()) {
                row_info.sstable_end_log_ts_ = table->get_end_log_ts();
              }
            }
          }
        }
      }
      // put fuse row cache
      if (OB_SUCC(ret) && access_ctx_->use_fuse_row_cache_) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = try_put_fuse_row_cache(row_info))) {
          STORAGE_LOG(WARN, "fail to try put fuse row cache", K(ret));
        }
      }
      ++get_row_range_idx_;

      // project row
      if (OB_SUCC(ret)) {
        if (ObActionFlag::OP_ROW_EXIST == row_info.row_.flag_) {
          if (access_ctx_->use_fuse_row_cache_) {
            row.row_val_.count_ = access_param_->iter_param_.projector_->count();
            if (OB_FAIL(
                    project_row(row_info.row_, access_param_->iter_param_.projector_, 0 /*range idx delta*/, row))) {
              STORAGE_LOG(WARN, "fail to project row", K(ret));
            }
          } else {
            row.row_val_.count_ = row_info.row_.row_val_.count_;
            if (OB_FAIL(project_row(row_info.row_, nullptr, 0 /*range idx delta*/, row))) {
              STORAGE_LOG(WARN, "fail to project row", K(ret));
            }
          }
          STORAGE_LOG(DEBUG, "multi get next row", K(row));
          break;
        }
      }
    }
  }
  return ret;
}

int ObMultipleGetMerge::inner_get_next_row_without_fuse_row_cache(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  bool reach_end = false;
  const ObStoreRow* tmp_row = NULL;
  bool final_result = false;
  access_ctx_->use_fuse_row_cache_ = false;
  if (OB_UNLIKELY(0 == iters_.count())) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret)) {
      final_result = false;
      ObStoreRow& fuse_row = row;
      nop_pos_.reset();
      fuse_row.row_val_.count_ = 0;
      fuse_row.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
      fuse_row.from_base_ = false;
      fuse_row.snapshot_version_ = 0L;

      for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
        if (OB_FAIL(iters_[i]->get_next_row(tmp_row))) {
          // check if all iterators return OB_ITER_END
          if (OB_ITER_END == ret && (0 == i || reach_end)) {
            reach_end = true;
            if (i < iters_.count() - 1) {
              ret = OB_SUCCESS;
            }
          } else {
            STORAGE_LOG(WARN, "Iterator get next row failed", K(i), K(ret));
          }
        } else if (OB_ISNULL(tmp_row)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tmp_row is NULL", K(ret));
        } else {
          // fuse working row and result row
          fuse_row.scan_index_ = tmp_row->scan_index_;
          if (!final_result) {
            if (OB_FAIL(ObRowFuse::fuse_row(*tmp_row, fuse_row, nop_pos_, final_result))) {
              STORAGE_LOG(WARN, "failed to merge rows", K(*tmp_row), K(row), K(ret));
            }
          }
        }
      }
      if (OB_SUCCESS == ret && ObActionFlag::OP_ROW_EXIST == fuse_row.flag_) {
        // find result
        STORAGE_LOG(DEBUG, "Success to merge get row, ", KP(this), K(fuse_row));
        break;
      }
    }
  }
  return ret;
}

void ObMultipleGetMerge::collect_merge_stat(ObTableStoreStat& stat) const
{
  stat.multi_get_stat_.call_cnt_++;
  stat.multi_get_stat_.output_row_cnt_ += table_stat_.output_row_cnt_;
  if (access_ctx_->query_flag_.is_index_back()) {
    stat.index_back_stat_.call_cnt_++;
    stat.index_back_stat_.output_row_cnt_ += table_stat_.output_row_cnt_;
  }
}

int ObMultipleGetMerge::inner_get_next_row(ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (can_prefetch_all_) {
    if (OB_FAIL(inner_get_next_row_with_fuse_row_cache(row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to inner get next row with fuse row cache", K(ret));
      }
    }
  } else {
    if (OB_FAIL(inner_get_next_row_without_fuse_row_cache(row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to inner get next row without fuse row cache", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    row.range_array_idx_ = rowkeys_->at(row.scan_index_).get_range_array_idx();
  }

  return ret;
}

int ObMultipleGetMerge::to_collation_free_rowkey_on_demand(
    const ObIArray<ObExtStoreRowkey>& rowkeys, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (0 == rowkeys.count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid rowkeys count", K(ret), K(rowkeys.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
    if (OB_FAIL(const_cast<ObExtStoreRowkey&>(rowkeys.at(i)).to_collation_free_on_demand_and_cutoff_range(allocator))) {
      STORAGE_LOG(WARN, "fail to get collation free rowkey", K(ret), K(rowkeys.at(i).get_store_rowkey()));
    }
  }
  return ret;
}

int ObMultipleGetMerge::estimate_row_count(const ObQueryFlag query_flag, const uint64_t table_id,
    const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, const common::ObIArray<ObITable*>& tables,
    ObPartitionEst& part_estimate)
{
  int ret = OB_SUCCESS;
  UNUSED(query_flag);
  if (OB_UNLIKELY(!is_valid_id(table_id)) || OB_UNLIKELY(rowkeys.count() <= 0) || OB_UNLIKELY(tables.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(table_id), K(rowkeys), K(tables.count()));
  } else {
    part_estimate.logical_row_count_ = part_estimate.physical_row_count_ = rowkeys.count();
  }
  return ret;
}

int ObMultipleGetMerge::skip_to_range(const int64_t range_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(range_idx));
  } else if (range_idx >= rowkeys_->count()) {
    ret = OB_ITER_END;
  } else {
    const ObStoreRowkey& rowkey = rowkeys_->at(range_idx).get_store_rowkey();
    for (int64_t i = iters_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (OB_FAIL(iters_[i]->skip_range(range_idx, &rowkey, true /*include gap key*/))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "skip range failed", K(ret));
        }
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
