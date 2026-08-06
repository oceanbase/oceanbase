/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_di_base_sstable_row_scanner.h"
#include "ob_aggregated_store.h"
#include "ob_aggregated_store_vec.h"

namespace oceanbase
{
namespace storage
{

ObDIBaseSSTableRowScanner::StashInfo::StashInfo()
    : allocator_(ObMemAttr(MTL_ID(), "DIBaseStash")),
      tablet_iter_(),
      tables_(),
      tables_handle_(),
      rowkey_read_info_(nullptr),
      read_info_(nullptr),
      ranges_(),
      multi_ranges_()
{
  tables_.set_block_allocator(ModulePageAllocator(allocator_));
  ranges_.set_block_allocator(ModulePageAllocator(allocator_));
  multi_ranges_.set_block_allocator(ModulePageAllocator(allocator_));
}

ObDIBaseSSTableRowScanner::StashInfo::~StashInfo()
{
  reset();
}

void ObDIBaseSSTableRowScanner::StashInfo::reset()
{
  tables_.reset();
  tablet_iter_.reset();
  tables_handle_.reset();
  if (nullptr != rowkey_read_info_) {
    rowkey_read_info_->reset();
    rowkey_read_info_->~ObRowkeyReadInfo();
    allocator_.free(rowkey_read_info_);
    rowkey_read_info_ = nullptr;
  }
  read_info_ = nullptr;
  ranges_.reset();
  for (int64_t i = 0; i < multi_ranges_.count(); ++i) {
    ObArray<ObDatumRange> *&range = multi_ranges_.at(i);
    if (OB_NOT_NULL(range)) {
      range->~ObArray<ObDatumRange>();
      allocator_.free(range);
      range = nullptr;
    }
  }
  multi_ranges_.reset();
  allocator_.reset();
}

int ObDIBaseSSTableRowScanner::StashInfo::deep_copy_sstables(
    const ObIArray<ObITable::TableKey> &table_keys,
    const int64_t start_pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_iter_.is_valid() ||
                  tables_.empty() ||
                  table_keys.empty() ||
                  start_pos < 0 ||
                  start_pos > table_keys.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_iter_), K(tables_),
             K(table_keys), K(start_pos));
  } else {
    ObTableHandleV2 table_handle;
    int64_t table_pos = 0;
    for (int64_t i = start_pos; OB_SUCC(ret) && i < table_keys.count(); ++i) {
      const ObITable::TableKey &table_key = table_keys.at(i);
      ObSSTable *orig_sstable = nullptr;
      ObSSTable *copied_sstable = nullptr;
      while (OB_SUCC(ret) && OB_ISNULL(orig_sstable) && table_pos < tables_.count()) {
        ObITable *table = tables_.at(table_pos);
        if (OB_ISNULL(table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null stashed table", K(ret), K(table_pos), K(i), K(table_key));
        } else if (table->get_key() == table_key) {
          orig_sstable = static_cast<ObSSTable *>(table);
        }
        ++table_pos;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(orig_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to find sstable in stashed tables", K(ret), K(i), K(table_key), K(tables_));
      } else if (OB_FAIL(ObSSTable::copy_from_old_sstable(*orig_sstable, allocator_, copied_sstable))) {
        LOG_WARN("failed to copy from old sstable", K(ret), KPC(orig_sstable), KP(copied_sstable));
      } else if (OB_FAIL(table_handle.set_sstable(copied_sstable, &allocator_))) {
        LOG_WARN("failed to set sstable", K(ret), KPC(copied_sstable));
      } else if (OB_FAIL(tables_handle_.add_table(table_handle))) {
        LOG_WARN("failed to add table", K(ret));
      }
    }
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::StashInfo::deep_copy_rowkey_read_info(
    const ObRowkeyReadInfo &rowkey_read_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rowkey_read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(rowkey_read_info));
  } else if (OB_NOT_NULL(rowkey_read_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey read info already exists", K(ret), K(rowkey_read_info_));
  } else {
    const int64_t rowkey_info_copy_size = rowkey_read_info.get_deep_copy_size();
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(rowkey_info_copy_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", K(ret), K(rowkey_info_copy_size));
    } else if (OB_FAIL(rowkey_read_info.deep_copy(buf, rowkey_info_copy_size, rowkey_read_info_))) {
      LOG_WARN("fail to deep copy rowkey read info", K(ret), K(rowkey_read_info));
    } else if (OB_UNLIKELY(nullptr == rowkey_read_info_ || !rowkey_read_info_->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected copied rowkey read info", K(ret), K(rowkey_read_info), KPC(rowkey_read_info_));
    } else {
      read_info_ = rowkey_read_info_;
    }
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::StashInfo::prepare_ranges(const bool is_multi_scan, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret),  K(size));
  } else if (is_multi_scan) {
    if (OB_FAIL(multi_ranges_.prepare_allocate(size))) {
      LOG_WARN("fail to prepare allocate multi ranges", K(ret), K(size));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      ObArray<ObDatumRange> *&range = multi_ranges_.at(i);
      if (OB_ISNULL(range = OB_NEWx(ObArray<ObDatumRange>, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate range array", K(ret));
      } else {
        range->set_block_allocator(ModulePageAllocator(allocator_));
      }
    }
  } else {
    if (OB_FAIL(ranges_.prepare_allocate(size))) {
      LOG_WARN("fail to prepare allocate ranges", K(ret), K(size));
    }
  }
  return ret;
}

ObDIBaseSSTableRowScanner::ObDIBaseSSTableRowScanner()
    : ObStoreRowIterator(),
      access_param_(nullptr),
      access_ctx_(nullptr),
      tables_(nullptr),
      is_multi_scan_(false),
      is_di_base_iter_end_(true),
      curr_di_base_idx_(0),
      di_base_table_keys_(),
      di_base_iters_(),
      di_base_curr_rowkey_(),
      di_base_curr_scan_index_(0),
      di_base_range_(nullptr),
      di_base_cow_range_(),
      di_base_multi_range_(nullptr),
      di_base_cow_multi_range_(),
      stash_info_(nullptr)
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
  is_multi_scan_ = false;
  is_di_base_iter_end_ = true;
  curr_di_base_idx_ = 0;
  di_base_table_keys_.reset();
  di_base_curr_rowkey_.reset();
  di_base_curr_scan_index_ = 0;
  di_base_range_ = nullptr;
  di_base_cow_range_.reset();
  di_base_multi_range_ = nullptr;
  di_base_cow_multi_range_.reset();
  if (nullptr != stash_info_) {
    stash_info_->~StashInfo();
    stash_info_ = nullptr;
  }
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
  // alloc from access_ctx_->allocator_, need destroy
  if (nullptr != stash_info_) {
    stash_info_->~StashInfo();
    stash_info_ = nullptr;
  }
}

int ObDIBaseSSTableRowScanner::get_next_rows()
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator *iter = nullptr;
  ObBlockRowStore *block_row_store = access_ctx_->block_row_store_;
  ObAggregatedStoreVec *agg_store_vec = access_param_->iter_param_.enable_pd_aggregate() && access_param_->iter_param_.plan_use_new_format()
      ? static_cast<ObAggregatedStoreVec *>(block_row_store) : nullptr;
  while (OB_SUCC(ret) && !block_row_store->is_end() && curr_di_base_idx_ < get_di_base_iter_cnt()) {
    iter = get_di_base_iter(curr_di_base_idx_);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("di base iter is null", K(ret), K(curr_di_base_idx_), K(di_base_iters_));
    } else if (nullptr != agg_store_vec && OB_FAIL(agg_store_vec->prepare_batch_scan())) {
      LOG_WARN("Fail to prepare batch scan", K(ret), KPC(agg_store_vec));
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
  LOG_DEBUG("refresh blockscan checker", K(ret), K(get_di_base_iter_cnt()), K(border_rowkey));
  return ret;
}

int ObDIBaseSSTableRowScanner::switch_param(ObTableAccessParam *access_param,
                                            ObTableAccessContext *access_ctx,
                                            const common::ObIArray<ObITable *> &tables,
                                            const bool is_multi_scan)
{
  int ret = OB_SUCCESS;
  access_param_ = access_param;
  access_ctx_ = access_ctx;
  tables_ = &tables;
  is_multi_scan_ = is_multi_scan;
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

int ObDIBaseSSTableRowScanner::check_di_base_changed(const common::ObIArray<ObITable *> &tables, bool &is_changed)
{
  int ret = OB_SUCCESS;
  is_changed = false;
  tables_ = &tables;
  ObITable *table = nullptr;
  int64_t di_base_idx = 0;
  for (int64_t idx = 0; OB_SUCC(ret) && !is_changed && idx < tables.count(); ++idx) {
    if (OB_FAIL(tables.at(idx, table))) {
      STORAGE_LOG(WARN, "fail to get table", K(ret), K(idx));
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get unexpected null table", K(ret), K(idx));
    } else if (table->is_major_type_sstable()) {
      if (OB_UNLIKELY(di_base_idx >= get_di_base_table_cnt())) {
        is_changed = true;
        FLOG_INFO("inc major count changed after refresh, retry to scan", K(ret), K(di_base_idx), K(get_di_base_table_cnt()));
      } else if (OB_UNLIKELY(table->get_key() != get_di_base_table_key(di_base_idx))) {
        is_changed = true;
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
    is_changed = true;
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

int ObDIBaseSSTableRowScanner::prepare_ranges(const blocksstable::ObDatumRange &range, const bool copy_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_multi_scan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected in multi scan", K(ret));
  } else if (OB_UNLIKELY(!range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(range));
  } else if (copy_ranges) {
    di_base_cow_range_ = range;
    di_base_range_ = &di_base_cow_range_;
  } else {
    di_base_range_ = &range;
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::prepare_ranges(const common::ObIArray<blocksstable::ObDatumRange> &ranges, const bool copy_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_multi_scan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected in single scan", K(ret));
  } else if (OB_UNLIKELY(ranges.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ranges));
  } else if (copy_ranges) {
    if (OB_FAIL(di_base_cow_multi_range_.assign(ranges))) {
      LOG_WARN("fail to assign di base cow multi range", K(ret), K(ranges));
    } else {
      di_base_multi_range_ = &di_base_cow_multi_range_;
    }
  } else {
    di_base_multi_range_ = &ranges;
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::calc_scan_range()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!di_base_curr_rowkey_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid curr rowkey", K(ret), K(di_base_curr_rowkey_));
  } else if (is_multi_scan_) {
    int64_t range_idx_delta = 0;
    if (OB_FAIL(ObMultipleMerge::calc_scan_range_by_rowkey(access_param_,
                                                           access_ctx_,
                                                           di_base_multi_range_,
                                                           di_base_cow_multi_range_,
                                                           di_base_curr_scan_index_,
                                                           di_base_curr_rowkey_,
                                                           range_idx_delta,
                                                           true/*calc_di_base_range*/))) {
      LOG_WARN("fail to calculate scan range", K(ret));
    } else {
      STORAGE_LOG(INFO, "calculate di base scan range", K(ret), KPC(di_base_multi_range_), K(di_base_curr_scan_index_), K(di_base_curr_rowkey_));
    }
  } else {
    if (OB_FAIL(ObMultipleMerge::calc_scan_range_by_rowkey(access_ctx_,
                                                           di_base_range_,
                                                           di_base_cow_range_,
                                                           di_base_curr_rowkey_,
                                                           true/*calc_di_base_range*/))) {
      LOG_WARN("fail to calculate scan range", K(ret));
    } else {
      STORAGE_LOG(INFO, "calculate di base scan range", K(ret), KPC(di_base_range_), K(di_base_curr_rowkey_));
    }
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::construct_iters()
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
        if (is_multi_scan_ && OB_FAIL(table->multi_scan(*iter_param,
                                                        *access_ctx_,
                                                        *di_base_multi_range_,
                                                        iter))) {
          LOG_WARN("Fail to get di base iterator", K(ret), KPC(table), K(*iter_param), K(idx), K(di_base_idx));
        } else if (!is_multi_scan_ && OB_FAIL(table->scan(*iter_param,
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
      } else if (is_multi_scan_ && OB_FAIL(iter->init(*iter_param,
                                                      *access_ctx_,
                                                      table,
                                                      di_base_multi_range_))) {
        LOG_WARN("failed to init scan di_base_iters_", K(ret), K(idx), K(di_base_idx));
      } else if (!is_multi_scan_ && OB_FAIL(iter->init(*iter_param,
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

int ObDIBaseSSTableRowScanner::save_curr_rowkey(const bool need_scan_di_base)
{
  int ret = OB_SUCCESS;
  // save di base curr rowkey in fast refresh table, scan idx and border rowkey
  di_base_curr_rowkey_.reset();
  di_base_curr_scan_index_ = 0;
  const int64_t di_base_table_cnt = get_di_base_table_cnt();
  if (!need_scan_di_base) {
    // di base iter end, do nothing
  } else if (OB_UNLIKELY(di_base_table_cnt > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected di base table count", K(ret), K(di_base_table_cnt));
  } else { // di_base_table_cnt == 1
    ObAggStoreBase *agg_store = nullptr;
    if (access_param_->iter_param_.enable_pd_aggregate()) {
      if (access_param_->iter_param_.plan_use_new_format()) {
        agg_store = static_cast<ObAggregatedStoreVec *>(access_ctx_->block_row_store_);
      } else {
        agg_store = static_cast<ObAggregatedStore *>(access_ctx_->block_row_store_);
      }
    }
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
    }
    if (nullptr != agg_store) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(agg_store->set_ignore_eval_index_info(false))) { // enable eval index info
        LOG_WARN("Failed to set ignore eval index info", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
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

int ObDIBaseSSTableRowScanner::stash_tablet_iter(const ObTabletTableIterator &tablet_iter, const ObIArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_iter.is_valid() || tables.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_iter), K(tables));
  } else if (nullptr == stash_info_ &&
             OB_ISNULL(stash_info_ = OB_NEWx(StashInfo, access_ctx_->allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate drain refresh info", K(ret));
  } else if (OB_FAIL(stash_info_->tablet_iter_.assign(tablet_iter))) {
    LOG_WARN("fail to assign tablet iterator", K(ret), K(tablet_iter));
  } else if (OB_FAIL(stash_info_->tables_.assign(tables))) {
    LOG_WARN("fail to assign tables", K(ret), K(tables));
  } else {
    stash_info_->read_info_ = &(stash_info_->tablet_iter_.get_tablet()->get_rowkey_read_info());
  }
  return ret;
}

int ObDIBaseSSTableRowScanner::clone_and_switch_tables(const ObDatumRowkey &border_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stash_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stash info", K(ret));
  } else if (OB_UNLIKELY(!stash_info_->tablet_iter_.is_valid() ||
                         stash_info_->tables_.empty() ||
                         nullptr == stash_info_->read_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stash info", K(ret), KPC(stash_info_));
  } else if (OB_UNLIKELY(di_base_table_keys_.empty() || di_base_iters_.empty() ||
                         di_base_table_keys_.count() != di_base_iters_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no di base table keys or iter count mismatch", K(ret),
             K(di_base_table_keys_), K(di_base_iters_));
  } else {
    // copy di base sstables and rowkey read info from old tablet
    if (OB_FAIL(stash_info_->deep_copy_sstables(di_base_table_keys_, curr_di_base_idx_))) {
      LOG_WARN("fail to deep copy sstables", K(ret), KPC(stash_info_), K(di_base_table_keys_), K(curr_di_base_idx_));
    } else if (OB_FAIL(stash_info_->deep_copy_rowkey_read_info(
          static_cast<const ObRowkeyReadInfo &>(*stash_info_->read_info_)))) {
      LOG_WARN("fail to deep copy rowkey read info", K(ret), K(stash_info_->tablet_iter_));
    } else if (OB_FAIL(stash_info_->prepare_ranges(is_multi_scan_, di_base_table_keys_.count() - curr_di_base_idx_))) {
      LOG_WARN("fail to prepare ranges", K(ret), K(di_base_table_keys_), K(curr_di_base_idx_));
    } else {
      access_param_->iter_param_.rowkey_read_info_ = stash_info_->rowkey_read_info_;
    }
    // reinit iters
    if (OB_SUCC(ret)) {
      int64_t curr_scan_index = 0;
      ObDatumRowkey next_rowkey;
      ObAggStoreBase *agg_store = nullptr;
      if (access_param_->iter_param_.enable_pd_aggregate()) {
        if (access_param_->iter_param_.plan_use_new_format()) {
          agg_store = static_cast<ObAggregatedStoreVec *>(access_ctx_->block_row_store_);
        } else {
          agg_store = static_cast<ObAggregatedStore *>(access_ctx_->block_row_store_);
        }
      }
      if (nullptr != agg_store && OB_FAIL(agg_store->set_ignore_eval_index_info(true))) { // disable eval index info temporarily
        LOG_WARN("Failed to set ignore eval index info", K(ret));
      }
      for (int64_t i = curr_di_base_idx_; OB_SUCC(ret) && i < di_base_iters_.count(); ++i) {
        const int64_t idx = i - curr_di_base_idx_; // idx in stash info
        ObITable *table = nullptr;
        ObStoreRowIterator *iter = nullptr;
        if (OB_ISNULL(table = stash_info_->tables_handle_.get_table(idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected sstable is null", K(ret), K(idx), K(stash_info_->tables_handle_));
        } else if (OB_ISNULL(iter = di_base_iters_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected di base iter is null", K(ret), K(i), K(di_base_iters_));
        } else if (OB_FAIL(iter->get_next_rowkey(curr_scan_index, next_rowkey, stash_info_->allocator_))) {
          LOG_WARN("fail to get di base next rowkey", K(ret), KPC(iter));
        } else if (FALSE_IT(iter->reuse())) {
        } else if (is_multi_scan_) {
          const ObIArray<ObDatumRange> *di_base_multi_range = di_base_multi_range_;
          ObArray<ObDatumRange> &di_base_cow_multi_range = *stash_info_->multi_ranges_.at(idx);
          int64_t range_idx_delta = 0;
          if (OB_FAIL(ObMultipleMerge::calc_scan_range_by_rowkey(access_param_,
                                                                 access_ctx_,
                                                                 di_base_multi_range,
                                                                 di_base_cow_multi_range,
                                                                 curr_scan_index,
                                                                 next_rowkey,
                                                                 range_idx_delta,
                                                                 true/*calc_di_base_range*/))) {
            LOG_WARN("fail to calculate scan range", K(ret));
          } else {
            STORAGE_LOG(INFO, "calculate di base scan range", K(ret), K(i), K(curr_scan_index), K(next_rowkey), KPC(di_base_multi_range_), KPC(di_base_multi_range));
            if (OB_FAIL(iter->init(access_param_->iter_param_, *access_ctx_, table, di_base_multi_range))) {
              LOG_WARN("fail to init di base iter", K(ret), KPC(iter), KPC(table));
            } else if (OB_FAIL(iter->refresh_blockscan_checker(border_rowkey))) {
              LOG_WARN("fail to refresh blockscan checker", K(ret), K(border_rowkey), KPC(iter));
            }
          }
        } else {
          const ObDatumRange *di_base_range = di_base_range_;
          ObDatumRange &di_base_cow_range = stash_info_->ranges_.at(idx);
          if (OB_FAIL(ObMultipleMerge::calc_scan_range_by_rowkey(access_ctx_,
                                                                 di_base_range,
                                                                 di_base_cow_range,
                                                                 next_rowkey,
                                                                 true/*calc_di_base_range*/))) {
            LOG_WARN("fail to calculate scan range", K(ret));
          } else {
            STORAGE_LOG(INFO, "calculate di base scan range", K(ret), K(i), K(next_rowkey), KPC(di_base_range_), KPC(di_base_range));
            if (OB_FAIL(iter->init(access_param_->iter_param_, *access_ctx_, table, di_base_range))) {
              LOG_WARN("fail to init di base iter", K(ret), KPC(iter), KPC(table));
            } else if (OB_FAIL(iter->refresh_blockscan_checker(border_rowkey))) {
              LOG_WARN("fail to refresh blockscan checker", K(ret), K(border_rowkey), KPC(iter));
            }
          }
        }
      }
      if (nullptr != agg_store) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(agg_store->set_ignore_eval_index_info(false))) { // enable eval index info
          LOG_WARN("Failed to set ignore eval index info", K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
    // release old tablet
    if (OB_SUCC(ret)) {
      stash_info_->tables_.reset();
      stash_info_->tablet_iter_.reset();
    }
  }
  return ret;
}

void ObDIBaseSSTableRowScanner::clear_stash()
{
  if (nullptr != stash_info_) {
    stash_info_->reset();
  }
}

const ObITableReadInfo *ObDIBaseSSTableRowScanner::get_rowkey_read_info() const
{
  return nullptr != stash_info_ ? stash_info_->read_info_ : nullptr;
}

} // namespace storage
} // namespace oceanbase
