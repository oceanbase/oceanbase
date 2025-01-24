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

#include "ob_single_merge.h"
#include "storage/column_store/ob_co_sstable_row_getter.h"
#include "src/storage/ls/ob_ls.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

ObSingleMerge::ObSingleMerge()
  : rowkey_(NULL), full_row_(), handle_(), fuse_row_cache_fetcher_()
{
  type_ = ObQRIterType::T_SINGLE_GET;
}

ObSingleMerge::~ObSingleMerge()
{
}

int ObSingleMerge::open(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMultipleMerge::open())) {
    STORAGE_LOG(WARN, "Fail to open ObMultipleMerge, ", K(ret));
  } else if (OB_ISNULL(get_table_param_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSingleMerge has not been inited", K(ret), K_(get_table_param));
  } else {
    const ObTabletMeta &tablet_meta = get_table_param_->tablet_iter_.get_tablet()->get_tablet_meta();
    if (!full_row_.is_valid()) {
      if (OB_FAIL(full_row_.init(*long_life_allocator_, access_param_->get_max_out_col_cnt()))) {
        STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
      } else {
        full_row_.count_ = access_param_->get_max_out_col_cnt();
      }
    } else if (OB_FAIL(full_row_.reserve(access_param_->get_max_out_col_cnt()))) {
      STORAGE_LOG(WARN, "Failed to reserve full row", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fuse_row_cache_fetcher_.init(access_ctx_->get_scan_type(),
                                                    access_param_->iter_param_.tablet_id_,
                                                    access_param_->iter_param_.get_read_info(),
                                                    tablet_meta.clog_checkpoint_scn_.get_val_for_tx(),
                                                    access_ctx_->trans_version_range_.base_version_,
                                                    access_ctx_->trans_version_range_.snapshot_version_))) {
      STORAGE_LOG(WARN, "fail to init fuse row cache fetcher", K(ret));
    } else {
      rowkey_ = &rowkey;
    }
  }

  return ret;
}

void ObSingleMerge::reset()
{
  ObMultipleMerge::reset();
  rowkey_ = nullptr;
  full_row_.reset();
  handle_.reset();
}

void ObSingleMerge::reuse()
{
  ObMultipleMerge::reuse();
  full_row_.row_flag_.reset();
  rowkey_ = NULL;
  handle_.reset();
}

void ObSingleMerge::reclaim()
{
  ObMultipleMerge::reclaim();
  rowkey_ = nullptr;
  full_row_.row_flag_.reset();
  full_row_.trans_info_ = nullptr;
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

int ObSingleMerge::get_table_row(const int64_t table_idx,
                                 const ObIArray<ObITable *> &tables,
                                 ObDatumRow &fuse_row,
                                 bool &final_result,
                                 bool &has_uncommited_row)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator *iter = NULL;
  const ObTableIterParam *iter_param = nullptr;
  ObITable *table = nullptr;
  const ObDatumRow *prow = nullptr;
  if (OB_FAIL(tables.at(table_idx, table))) {
    STORAGE_LOG(WARN, "fail to get table", K(ret));
  } else if (OB_ISNULL(iter_param = get_actual_iter_param(table))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Fail to get access param", K(table_idx), K(ret), K(*table));
  } else if (iters_.count() < tables.count() - table_idx) {
    // this table has not been accessed before
    if (OB_FAIL(table->get(*iter_param, *access_ctx_, *rowkey_, iter))) {
      STORAGE_LOG(WARN, "Fail to get row, ", K(ret), K(table_idx),
          K(iters_.count()), K(tables.count()));
    } else if (OB_FAIL(iters_.push_back(iter))) {
      iter->~ObStoreRowIterator();
      STORAGE_LOG(WARN, "Fail to push iter to iterator array, ", K(ret), K(table_idx),
          K(iters_.count()), K(tables.count()));
    }
  } else {
    iter = iters_.at(tables.count() - table_idx - 1);
    if (OB_FAIL(iter->init(*iter_param, *access_ctx_, table, rowkey_))) {
      STORAGE_LOG(WARN, "failed to init get iter", K(ret), K(table_idx),
          K(iters_.count()), K(tables.count()));
    }
  }
  if (OB_SUCC(ret) && ObStoreRowIterator::IteratorCOSingleGet == iter->get_iter_type()
      && !fuse_row.row_flag_.is_not_exist() && 0 != fuse_row.count_) {
    reinterpret_cast<ObCOSSTableRowGetter *>(iter)->set_nop_pos(&nop_pos_);
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
      fuse_row.group_idx_ = 0;
      if (prow->row_flag_.is_exist() && !has_uncommited_row) {
        has_uncommited_row = prow->is_have_uncommited_row() || fuse_row.snapshot_version_ == INT64_MAX;
      }
      REALTIME_MONITOR_INC_READ_ROW_CNT(iter, access_ctx_);
      STORAGE_LOG(DEBUG, "process row fuse", K(ret), KPC(prow), K(fuse_row), KPC(access_ctx_->store_ctx_));
    }
  }
  return ret;
}

int ObSingleMerge::get_and_fuse_cache_row(const int64_t read_snapshot_version,
                                          const int64_t multi_version_start,
                                          ObDatumRow &fuse_row,
                                          bool &final_result,
                                          bool &have_uncommited_row,
                                          bool &need_update_fuse_cache)
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  int64_t end_table_idx = tables_.count();
  if (OB_UNLIKELY(final_result)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected call to get fuse cache row", K(ret), K(fuse_row), K(final_result));
  } else if (OB_FAIL(fuse_row_cache_fetcher_.get_fuse_row_cache(*rowkey_, handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get from fuse row cache", K(ret), KPC(rowkey_));
    } else {
      ++access_ctx_->table_store_stat_.fuse_row_cache_miss_cnt_;
      ret = OB_SUCCESS;
      end_table_idx = 0;
      need_update_fuse_cache = true;
    }
  } else if (OB_UNLIKELY(handle_.value_->get_read_snapshot_version() <= multi_version_start
                        || handle_.value_->get_read_snapshot_version() > read_snapshot_version)) {
    STORAGE_LOG(DEBUG, "fuse row cache useless", K(handle_), K(read_snapshot_version), KPC(rowkey_));
    handle_.reset();
    end_table_idx = 0;
    need_update_fuse_cache = true;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); i++) {
      if (OB_ISNULL(table = tables_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null table", K(ret), K(i), K(tables_));
      } else if (table->is_memtable()) {
        break;
      } else if (handle_.value_->get_read_snapshot_version() < table->get_upper_trans_version()) {
        end_table_idx = i;
        need_update_fuse_cache = true;
        break;
      }
    }
    if (OB_SUCC(ret) && end_table_idx == 0){
      handle_.reset();
    }
  }

  for (int64_t i = tables_.count() - 1; OB_SUCC(ret) && !final_result && i >= end_table_idx; --i) {
    if (OB_ISNULL(table = tables_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null table", K(ret), K(i), K(tables_));
    } else if (table->is_memtable()) {
    } else if (OB_FAIL(get_table_row(i, tables_, full_row_, final_result, have_uncommited_row))) {
      STORAGE_LOG(WARN, "fail to get table row", K(ret), K(i), K(full_row_), K(tables_));
    }
  }
  if (OB_SUCC(ret) && handle_.is_valid()) {
    ObDatumRow cache_row;
    cache_row.count_ = handle_.value_->get_column_cnt();
    cache_row.storage_datums_ = handle_.value_->get_datums();
    cache_row.row_flag_ = handle_.value_->get_flag();
    ++access_ctx_->table_store_stat_.fuse_row_cache_hit_cnt_;
    STORAGE_LOG(DEBUG, "find fuse row cache", K(handle_), KPC(rowkey_));
    if (cache_row.row_flag_.is_exist()) {
      if (OB_FAIL(ObRowFuse::fuse_row(cache_row, fuse_row, nop_pos_, final_result))) {
        STORAGE_LOG(WARN, "fail to fuse row", K(ret));
      } else {
        STORAGE_LOG(TRACE, "fuse row cache", K(cache_row), K(fuse_row));
      }
    }
  }

  return ret;
}

int ObSingleMerge::inner_get_next_row(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (NULL != rowkey_ && 0 < tables_.count()) {
    ObITable *table = tables_.at(0);
    bool have_uncommited_row = false;
    const StorageScanType scan_type = access_ctx_->get_scan_type();
    const ObITableReadInfo *read_info = access_param_->iter_param_.get_read_info();
    const ObTabletMeta &tablet_meta = get_table_param_->tablet_iter_.get_tablet()->get_tablet_meta();
    const int64_t read_snapshot_version = access_ctx_->trans_version_range_.snapshot_version_;
    const bool enable_fuse_row_cache = access_ctx_->use_fuse_row_cache_ &&
                                       access_param_->iter_param_.enable_fuse_row_cache(access_ctx_->query_flag_, scan_type) &&
                                       (is_mview_table_scan(scan_type) ||
                                        read_snapshot_version >= tablet_meta.snapshot_version_) &&
                                       (!table->is_co_sstable() || static_cast<ObCOSSTableV2 *>(table)->is_all_cg_base()) &&
                                       !tablet_meta.has_transfer_table(); // The query in the transfer scenario does not enable fuse row cache
    bool need_update_fuse_cache = false;
    access_ctx_->query_flag_.set_not_use_row_cache();
    nop_pos_.reset();
    full_row_.count_ = 0;
    full_row_.row_flag_.reset();
    full_row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    full_row_.snapshot_version_ = 0L;
    access_ctx_->use_fuse_row_cache_ = enable_fuse_row_cache;

    STORAGE_LOG(DEBUG, "single merge start to get next row", KPC(rowkey_), K(access_ctx_->use_fuse_row_cache_),
                K(access_param_->iter_param_.enable_fuse_row_cache(access_ctx_->query_flag_, scan_type)), K(access_param_->iter_param_));
    if (is_mview_table_scan(scan_type)) {
      if (OB_FAIL(get_mview_table_scan_row(enable_fuse_row_cache, have_uncommited_row, need_update_fuse_cache))) {
        STORAGE_LOG(WARN, "Failed to get mview table scan row", K(ret), K(enable_fuse_row_cache));
      }
    } else if (OB_FAIL(get_normal_table_scan_row(read_snapshot_version,
                                                 tablet_meta.multi_version_start_,
                                                 enable_fuse_row_cache,
                                                 have_uncommited_row,
                                                 need_update_fuse_cache))) {
      STORAGE_LOG(WARN, "Failed to get normal row", K(ret), K(read_snapshot_version), K(tablet_meta.multi_version_start_),
                  K(enable_fuse_row_cache));
    }

    if (OB_SUCC(ret)) {
      STORAGE_LOG(DEBUG, "row before project", K(iter_del_row_), K(full_row_));
      if (!full_row_.row_flag_.is_exist_without_delete() && !(iter_del_row_  && full_row_.row_flag_.is_delete())) {
        ret = OB_ITER_END;
      } else {
        const ObColumnIndexArray &cols_index = read_info->get_columns_index();
        row.count_ = read_info->get_request_count();
        const ObIArray<int32_t> *projector = (cols_index.rowkey_mode_ || !enable_fuse_row_cache) ? nullptr : &cols_index.array_;
        if (OB_FAIL(project_row(full_row_, projector, 0/*range idx delta*/, row))) {
          STORAGE_LOG(WARN, "fail to project row", K(ret), K(full_row_), K(cols_index));
        } else {
          row.row_flag_ = full_row_.row_flag_;
          row.group_idx_ = rowkey_->get_group_idx();
          row.trans_info_ = full_row_.trans_info_;
          STORAGE_LOG(TRACE, "succ to do single get", K(full_row_), K(row), K(have_uncommited_row), K(cols_index), K(access_param_->iter_param_.table_id_));
        }
        if (OB_FAIL(ret)) {
        } else if (!have_uncommited_row && need_update_fuse_cache
            && access_ctx_->enable_put_fuse_row_cache(SINGLE_GET_FUSE_ROW_CACHE_PUT_COUNT_THRESHOLD, is_mview_table_scan(scan_type))) {
          // try to put row cache
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = fuse_row_cache_fetcher_.put_fuse_row_cache(*rowkey_, full_row_))) {
            STORAGE_LOG(WARN, "fail to put fuse row cache", K(ret), KPC(rowkey_), K(full_row_), K(read_snapshot_version));
          } else {
            access_ctx_->table_store_stat_.fuse_row_cache_put_cnt_++;
          }
        }
      }
    }
    // When the index lookups the rowkeys from the main table, it should exists
    // and if we find that it does not exist, there must be an anomaly
    if (GCONF.enable_defensive_check()
        && access_ctx_->query_flag_.is_lookup_for_4377()
        && OB_ITER_END == ret) {
      ret = handle_4377("[index lookup]ObSingleMerge::inner_get_next_row");
      STORAGE_LOG(WARN, "[index lookup] row not found", K(ret),
                  K(have_uncommited_row),
                  K(enable_fuse_row_cache),
                  K(read_snapshot_version),
                  KPC(read_info),
                  K(tables_));
    }
    rowkey_ = NULL;
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObSingleMerge::get_normal_table_scan_row(const int64_t read_snapshot_version,
                                             const int64_t multi_version_start,
                                             const bool enable_fuse_row_cache,
                                             bool &have_uncommited_row,
                                             bool &need_update_fuse_cache)
{
  int ret = OB_SUCCESS;
  bool final_result = false;
  int64_t table_idx = -1;
  ObITable *table = nullptr;
  for (table_idx = tables_.count() - 1; OB_SUCC(ret) && !final_result && table_idx >= 0; --table_idx) {
    if (OB_ISNULL(table = tables_.at(table_idx))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null table to single get", K(ret), K(table_idx), K(tables_));
    } else if (!table->is_memtable()) {
      break;
    } else if (OB_FAIL(get_table_row(table_idx, tables_, full_row_, final_result, have_uncommited_row))) {
      STORAGE_LOG(WARN, "fail to get table row", K(ret), K(table_idx), K(full_row_), K(tables_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (final_result) {
  } else if (enable_fuse_row_cache) {
    if (OB_FAIL(get_and_fuse_cache_row(read_snapshot_version,
                                       multi_version_start,
                                       full_row_,
                                       final_result,
                                       have_uncommited_row,
                                       need_update_fuse_cache))) {
      STORAGE_LOG(WARN, "Failed to get fuse cache row", K(ret), K(full_row_));
    }
  } else {
    // secondly, try to get from other delta table
    for (; OB_SUCC(ret) && !final_result && table_idx >= 0; --table_idx) {
      if (OB_FAIL(get_table_row(table_idx, tables_, full_row_, final_result, have_uncommited_row))) {
        STORAGE_LOG(WARN, "fail to get table row", K(ret), K(table_idx), K(full_row_), K(tables_));
      }
    }
  }
  return ret;
}

int ObSingleMerge::get_mview_table_scan_row(const bool enable_fuse_row_cache,
                                            bool &have_uncommited_row,
                                            bool &need_update_fuse_cache)
{
  int ret = OB_SUCCESS;
  bool final_result = false;
  if (enable_fuse_row_cache) {
    if (OB_FAIL(fuse_row_cache_fetcher_.get_fuse_row_cache(*rowkey_, handle_))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "Failed to get from fuse row cache", K(ret), KPC(rowkey_));
      } else {
        ++access_ctx_->table_store_stat_.fuse_row_cache_miss_cnt_;
        ret = OB_SUCCESS;
      }
    } else if (handle_.is_valid()) {
      ObDatumRow cache_row;
      cache_row.count_ = handle_.value_->get_column_cnt();
      cache_row.storage_datums_ = handle_.value_->get_datums();
      cache_row.row_flag_ = handle_.value_->get_flag();
      ++access_ctx_->table_store_stat_.fuse_row_cache_hit_cnt_;
      STORAGE_LOG(DEBUG, "find fuse row cache", K(handle_), KPC(rowkey_));
      if (cache_row.row_flag_.is_exist()) {
        if (OB_FAIL(ObRowFuse::fuse_row(cache_row, full_row_, nop_pos_, final_result))) {
          STORAGE_LOG(WARN, "Failed to fuse row", K(ret));
        } else {
          STORAGE_LOG(TRACE, "fuse row cache", K(cache_row), K(full_row_), K(final_result));
          final_result = true;
        }
      }
    }
  }
  if (OB_SUCC(ret) && !final_result) {
    need_update_fuse_cache = enable_fuse_row_cache;
    int64_t table_idx = -1;
    ObITable *table = nullptr;
    for (table_idx = tables_.count() - 1; OB_SUCC(ret) && !final_result && table_idx >= 0; --table_idx) {
      if (OB_ISNULL(table = tables_.at(table_idx))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null table to single get", K(ret), K(table_idx), K(tables_));
      } else if (OB_FAIL(get_table_row(table_idx, tables_, full_row_, final_result, have_uncommited_row))) {
        STORAGE_LOG(WARN, "Failed to get table row", K(ret), K(table_idx), K(full_row_), K(tables_));
      }
    }
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
