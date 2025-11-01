/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_di_base_sstable_row_scanner.h"
#include "ob_aggregated_store.h"
#include "ob_aggregated_store_vec.h"

namespace oceanbase
{
namespace storage
{

ObDIBaseSSTableRowScanner::ObDIBaseSSTableRowScanner()
    : ObStoreRowIterator(),
      access_param_(nullptr),
      access_ctx_(nullptr),
      tables_(nullptr),
      is_di_base_iter_end_(true),
      curr_di_base_idx_(0),
      di_base_table_keys_(),
      di_base_iters_(),
      di_base_curr_rowkey_(),
      di_base_curr_scan_index_(0),
      di_base_range_(nullptr),
      di_base_cow_range_(),
      di_base_multi_range_(nullptr),
      di_base_cow_multi_range_()
{
  type_ = ObStoreRowIterator::IteratorScan;
}

ObDIBaseSSTableRowScanner::~ObDIBaseSSTableRowScanner()
{
  reset();
}

void ObDIBaseSSTableRowScanner::reset()
{
  reset_iter_array();
  access_param_ = nullptr;
  access_ctx_ = nullptr;
  tables_ = nullptr;
  is_di_base_iter_end_ = true;
  curr_di_base_idx_ = 0;
  di_base_table_keys_.reset();
  di_base_curr_rowkey_.reset();
  di_base_curr_scan_index_ = 0;
  di_base_range_ = nullptr;
  di_base_cow_range_.reset();
  di_base_multi_range_ = nullptr;
  di_base_cow_multi_range_.reset();
  ObStoreRowIterator::reset();
}

void ObDIBaseSSTableRowScanner::reuse()
{
  ObStoreRowIterator::reuse();
  reuse_iter_array();
  is_di_base_iter_end_ = true;
  curr_di_base_idx_ = 0;
  di_base_curr_rowkey_.reset();
  di_base_curr_scan_index_ = 0;
  di_base_cow_range_.reset();
  di_base_cow_multi_range_.reuse();
}

int ObDIBaseSSTableRowScanner::get_next_rows()
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator *iter = nullptr;
  ObBlockRowStore *block_row_store = access_ctx_->block_row_store_;
  while (OB_SUCC(ret) && !block_row_store->is_end() && curr_di_base_idx_ < get_di_base_iter_cnt()) {
    iter = get_di_base_iter(curr_di_base_idx_);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("di base iter is null", K(ret), K(curr_di_base_idx_), K(di_base_iters_));
    } else if (OB_FAIL(iter->get_next_rows())) {
      if (OB_UNLIKELY(OB_PUSHDOWN_STATUS_CHANGED != ret && OB_ITER_END != ret)) {
        LOG_WARN("fail to get next rows", K(ret), K(curr_di_base_idx_), K(di_base_iters_));
      } else {
        // TODO: zhanghuidong.zhd, reduce the number of blockscan interruptions
        // single di base iter status cannot determine the status of di base sstable row scanner
        // 1. set ret status to OB_ITER_END when all di base iters return OB_ITER_END
        // 2. set ret status to OB_PUSHDOWN_STATUS_CHANGED when finishing blockscan for all di base iters
        if (OB_PUSHDOWN_STATUS_CHANGED == ret) {
          is_di_base_iter_end_ = false;
          if (curr_di_base_idx_ != get_di_base_iter_cnt() - 1) {
            // continue to next di base iter
            ret = OB_SUCCESS;
          }
        } else {
          // OB_ITER_END
          if (curr_di_base_idx_ == get_di_base_iter_cnt() - 1) {
            if (is_di_base_iter_end_) {
              LOG_DEBUG("all di base iters are end", K(ret),
                                                     K(curr_di_base_idx_),
                                                     K(get_di_base_iter_cnt()),
                                                     K(di_base_iters_));
            } else {
              ret = OB_PUSHDOWN_STATUS_CHANGED;
              LOG_DEBUG("not all di base iters are end, and border rowkey is not found", K(ret),
                                                                                         K(curr_di_base_idx_),
                                                                                         K(get_di_base_iter_cnt()),
                                                                                         K(di_base_iters_));
            }
          } else {
            // continue to next di base iter
            ret = OB_SUCCESS;
          }
        }

        if (OB_SUCC(ret)) {
          ++curr_di_base_idx_;
          LOG_DEBUG("continue to next di base iter", K(ret),
                                                     K(curr_di_base_idx_),
                                                     K(get_di_base_iter_cnt()),
                                                     K(di_base_iters_));
        }
      }
    }
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::refresh_blockscan_checker(const blocksstable::ObDatumRowkey &border_rowkey)
{
  int ret = OB_SUCCESS;
  is_di_base_iter_end_ = true;
  curr_di_base_idx_ = 0;
  ObStoreRowIterator *iter = nullptr;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < get_di_base_iter_cnt(); ++idx) {
    iter = get_di_base_iter(idx);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("di base iter is null", K(ret), K(idx), K(di_base_iters_));
    } else if (OB_FAIL(iter->refresh_blockscan_checker(border_rowkey))) {
      LOG_WARN("fail to refresh blockscan checker", K(ret), K(border_rowkey), KPC(iter));
    }
  }
  FLOG_INFO("refresh blockscan checker", K(ret), K(get_di_base_iter_cnt()), K(border_rowkey));
  return ret;
}

int ObDIBaseSSTableRowScanner::switch_param(ObTableAccessParam *access_param,
                                            ObTableAccessContext *access_ctx,
                                            const common::ObIArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;
  access_param_ = access_param;
  access_ctx_ = access_ctx;
  tables_ = &tables;
  if (OB_ISNULL(access_param_) || OB_ISNULL(access_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(access_param), KP(access_ctx));
  } else {
    ObITable *table = nullptr;
    di_base_table_keys_.reuse();
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tables.count(); ++idx) {
      if (OB_ISNULL(table = tables.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret), K(idx), K(tables));
      } else if (table->is_major_type_sstable()) {
        if (OB_FAIL(di_base_table_keys_.push_back(table->get_key()))) {
          LOG_WARN("fail to push back di base table key", K(ret), K(idx), K(tables));
        }
      }
    }
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::prepare_di_base_blockscan(bool di_base_only, ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey border_rowkey;
  const int64_t rowkey_col_cnt = access_param_->iter_param_.get_schema_rowkey_count();
  if (di_base_only) {
    if (access_ctx_->query_flag_.is_reverse_scan()) {
      border_rowkey.set_min_rowkey();
    } else {
      border_rowkey.set_max_rowkey();
    }
  } else if (OB_ISNULL(row) || row->row_flag_.is_not_exist()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null or row_flag is not exist", K(ret));
  } else if (OB_FAIL(border_rowkey.assign(row->storage_datums_, rowkey_col_cnt))) {
    LOG_WARN("assign border_rowkey failed", K(ret), KPC(row), K(rowkey_col_cnt));
  } else {
    border_rowkey.scan_index_ = row->scan_index_;
  }

  if (FAILEDx(refresh_blockscan_checker(border_rowkey))) {
    LOG_WARN("fail to refresh blockscan checker", K(ret), K(border_rowkey));
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::check_di_base_changed(const common::ObIArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;
  tables_ = &tables;
  ObITable *table = nullptr;
  int64_t di_base_idx = 0;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < tables.count(); ++idx) {
    if (OB_FAIL(tables.at(idx, table))) {
      STORAGE_LOG(WARN, "fail to get table", K(ret), K(idx));
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get unexpected null table", K(ret), K(idx));
    } else if (table->is_major_type_sstable()) {
      if (OB_UNLIKELY(di_base_idx >= get_di_base_table_cnt())) {
        ret = OB_SNAPSHOT_DISCARDED;
        FLOG_INFO("inc major count changed after refresh, retry to scan", K(ret), K(di_base_idx), K(get_di_base_table_cnt()));
      } else if (OB_UNLIKELY(table->get_key() != get_di_base_table_key(di_base_idx))) {
        ret = OB_SNAPSHOT_DISCARDED;
        FLOG_INFO("table key of di base sstable changed after refresh, retry to scan", K(ret),
                                                                                       K(di_base_idx),
                                                                                       K(get_di_base_table_key(di_base_idx)),
                                                                                       KPC(table));
      } else {
        ++di_base_idx;
      }
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(di_base_idx != get_di_base_table_cnt())) {
    ret = OB_SNAPSHOT_DISCARDED;
    FLOG_INFO("inc major count changed after refresh, retry to scan", K(ret), K(di_base_idx), K(get_di_base_table_cnt()));
  }
  return ret;
}

void ObDIBaseSSTableRowScanner::reset_iter_array()
{
  ObStoreRowIterator *iter = nullptr;
  ObIAllocator *long_life_allocator = nullptr != access_ctx_ ? access_ctx_->get_long_life_allocator() : nullptr;
  for (int64_t i = 0; i < get_di_base_iter_cnt(); ++i) {
    if (OB_NOT_NULL(iter = di_base_iters_.at(i))) {
      iter->~ObStoreRowIterator();
      if (OB_NOT_NULL(long_life_allocator)) {
        long_life_allocator->free(iter);
      }
      iter = nullptr;
    }
  }
  di_base_iters_.reset();
}

void ObDIBaseSSTableRowScanner::reuse_iter_array()
{
  ObStoreRowIterator *iter = nullptr;
  ObStoreRowIterPool<ObStoreRowIterator> *stmt_iter_pool = nullptr != access_ctx_ ? access_ctx_->get_stmt_iter_pool() : nullptr;
  for (int64_t i = 0; i < get_di_base_iter_cnt(); ++i) {
    if (OB_NOT_NULL(iter = di_base_iters_.at(i))) {
      iter->reuse();
      if (nullptr != stmt_iter_pool) {
        stmt_iter_pool->return_iter(iter);
      }
    }
  }

  if (nullptr != stmt_iter_pool) {
    di_base_iters_.reuse();
  }
}

void ObDIBaseSSTableRowScanner::reclaim_iter_array()
{
  ObStoreRowIterator *iter = nullptr;
  ObStoreRowIterPool<ObStoreRowIterator> *stmt_iter_pool = nullptr != access_ctx_ ? access_ctx_->get_stmt_iter_pool() : nullptr;
  for (int64_t i = 0; i < get_di_base_iter_cnt(); ++i) {
    if (OB_NOT_NULL(iter = di_base_iters_.at(i))) {
      iter->reclaim();
      stmt_iter_pool->return_iter(iter);
    }
  }
  di_base_iters_.reuse();
}

int ObDIBaseSSTableRowScanner::prepare_ranges(const blocksstable::ObDatumRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(range));
  } else {
    di_base_range_ = &range;
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::prepare_ranges(const common::ObIArray<blocksstable::ObDatumRange> &ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ranges.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ranges));
  } else {
    di_base_multi_range_ = &ranges;
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::construct_iters(const bool is_multi_scan)
{
  int ret = OB_SUCCESS;
  const bool use_cache_iter = get_di_base_iter_cnt() > 0;
  int64_t di_base_idx = 0;
  ObITable *table = nullptr;
  ObStoreRowIterator *iter = nullptr;
  const ObTableIterParam *iter_param = &access_param_->iter_param_;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < get_table_cnt(); ++idx) {
    if (OB_ISNULL(table = tables_->at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get unexpected null table", K(ret), K(idx));
    } else if (table->is_major_type_sstable()) {
      if (!use_cache_iter) {
        if (is_multi_scan && OB_FAIL(table->multi_scan(*iter_param,
                                                        *access_ctx_,
                                                        *di_base_multi_range_,
                                                        iter))) {
          LOG_WARN("Fail to get di base iterator", K(ret), KPC(table), K(*iter_param), K(idx), K(di_base_idx));
        } else if (!is_multi_scan && OB_FAIL(table->scan(*iter_param,
                                                          *access_ctx_,
                                                          *di_base_range_,
                                                          iter))) {
          LOG_WARN("Fail to get di base iterator", K(ret), KPC(table), K(*iter_param), K(idx), K(di_base_idx));
        } else if (OB_FAIL(di_base_iters_.push_back(iter))) {
          iter->~ObStoreRowIterator();
          LOG_WARN("Fail to push di base iter to di base iterator array", K(ret));
        } else {
          ++di_base_idx;
        }
      } else if (OB_ISNULL(iter = di_base_iters_.at(di_base_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null di base iter", K(ret), K(idx), K(di_base_idx), K(di_base_iters_));
      } else if (is_multi_scan && OB_FAIL(iter->init(*iter_param,
                                                      *access_ctx_,
                                                      table,
                                                      di_base_multi_range_))) {
        LOG_WARN("failed to init scan di_base_iters_", K(ret), K(idx), K(di_base_idx));
      } else if (!is_multi_scan && OB_FAIL(iter->init(*iter_param,
                                                      *access_ctx_,
                                                      table,
                                                      di_base_range_))) {
        LOG_WARN("failed to init scan di_base_iters_", K(ret), K(idx), K(di_base_idx));
      } else {
        ++di_base_idx;
      }
    }
  }
  LOG_DEBUG("construct di base iters", K(ret), K(di_base_table_keys_), K(get_di_base_iter_cnt()), K(di_base_iters_));
  return ret;
}

int ObDIBaseSSTableRowScanner::save_curr_rowkey()
{
  int ret = OB_SUCCESS;
  ObAggStoreBase *agg_store = nullptr;
  if (access_param_->iter_param_.enable_pd_aggregate()) {
    if (access_param_->iter_param_.plan_use_new_format()) {
      agg_store = static_cast<ObAggregatedStoreVec *>(access_ctx_->block_row_store_);
    } else {
      agg_store = static_cast<ObAggregatedStore *>(access_ctx_->block_row_store_);
    }
  }
  // save di base curr rowkey, scan idx and border rowkey
  di_base_curr_rowkey_.reset();
  di_base_curr_scan_index_ = 0;
  if (get_di_base_iter_cnt() < 1 || curr_di_base_idx_ >= get_di_base_iter_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected di base iter idx", K(ret));
  } else {
    ObStoreRowIterator *iter = get_di_base_iter(curr_di_base_idx_);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected di base iter", K(curr_di_base_idx_));
    } else if (nullptr != agg_store && OB_FAIL(agg_store->set_ignore_eval_index_info(true))) { // disable eval index info temporarily
      LOG_WARN("Failed to set ignore eval index info", K(ret));
    } else if (OB_FAIL(iter->get_next_rowkey(di_base_curr_scan_index_,
                                             di_base_curr_rowkey_,
                                             *access_ctx_->allocator_))) {
      if (OB_ERR_UNSUPPORTED_TYPE != ret) {
        LOG_WARN("fail to get di base rowkey", K(ret), KPC(iter));
      }
    } else if (nullptr != agg_store && OB_FAIL(agg_store->set_ignore_eval_index_info(false))) {
      LOG_WARN("Failed to set ignore eval index info", K(ret));
    }
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::get_blockscan_border_rowkey(blocksstable::ObDatumRowkey &border_rowkey)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator *iter = get_di_base_iter(curr_di_base_idx_);
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("di base iter is null", K(ret), K(di_base_iters_));
  } else if (OB_FAIL(iter->get_blockscan_border_rowkey(border_rowkey))) {
    LOG_WARN("fail to get blockscan border rowkey", K(ret), K(curr_di_base_idx_));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase