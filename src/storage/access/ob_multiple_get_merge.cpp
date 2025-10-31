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
#include "ob_multiple_get_merge.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_meta.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
namespace storage
{
bool is_memtable(const ObITable *table)
{
  return table->is_memtable();
}

bool is_sstable(const ObITable *table)
{
  return !table->is_memtable();
}

ObMultipleGetMerge::ObMultipleGetMerge()
  : rowkeys_(nullptr),
    fuse_row_cache_fetcher_(),
    cache_handles_(),
    full_rows_(nullptr),
    prefetch_row_range_idx_(0),
    get_row_range_idx_(0),
    all_in_memory_(false)
{
  type_ = ObQRIterType::T_MULTI_GET;
}

ObMultipleGetMerge::~ObMultipleGetMerge()
{
  reset();
}

int ObMultipleGetMerge::open(const common::ObIArray<ObDatumRowkey> &rowkeys)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rowkeys.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument to do multi get", K(ret), K(rowkeys));
  } else if (OB_FAIL(ObMultipleMerge::open())) {
    STORAGE_LOG(WARN, "fail to open ObMultipleMerge, ", K(ret));
  } else {
    rowkeys_ = const_cast<common::ObIArray<blocksstable::ObDatumRowkey>*>(&rowkeys);
    if (OB_FAIL(construct_iters())) {
      STORAGE_LOG(WARN, "fail to construct iters", K(ret));
    } else {
      get_row_range_idx_ = 0;
      STORAGE_LOG(DEBUG, "success to open ObMultipleGetMerge", K(rowkeys));
    }
  }

  return ret;
}

void ObMultipleGetMerge::reset()
{
  rowkeys_ = nullptr;
  if (OB_NOT_NULL(full_rows_)) {
    for (int64_t i = 0; i < common::OB_MULTI_GET_OPEN_ROWKEY_NUM; ++i) {
      cache_handles_[i].reset();
      full_rows_[i].reset();
    }
    long_life_allocator_->free(full_rows_);
    full_rows_ = nullptr;
  }
  ObMultipleMerge::reset();
  prefetch_row_range_idx_ = 0;
  get_row_range_idx_ = 0;
  all_in_memory_ = false;
}

void ObMultipleGetMerge::reuse()
{
  ObMultipleMerge::reuse();
  if (OB_NOT_NULL(full_rows_)) {
    for (int64_t i = 0; i < common::OB_MULTI_GET_OPEN_ROWKEY_NUM; ++i) {
      cache_handles_[i].reset();
      full_rows_[i].reuse();
    }
  }
  prefetch_row_range_idx_ = 0;
  get_row_range_idx_ = 0;
}

void ObMultipleGetMerge::reclaim()
{
  ObMultipleMerge::reclaim();
  rowkeys_ = nullptr;
  if (OB_NOT_NULL(full_rows_)) {
    for (int64_t i = 0; i < common::OB_MULTI_GET_OPEN_ROWKEY_NUM; ++i) {
      cache_handles_[i].reset();
      full_rows_[i].reclaim();
    }
  }
  prefetch_row_range_idx_ = 0;
  get_row_range_idx_ = 0;
  all_in_memory_ = false;
}

int ObMultipleGetMerge::prepare()
{
  if (OB_NOT_NULL(full_rows_)) {
    for (int64_t i = 0; i < common::OB_MULTI_GET_OPEN_ROWKEY_NUM; ++i) {
      cache_handles_[i].reset();
      full_rows_[i].reuse();
    }
  }
  prefetch_row_range_idx_ = 0;
  get_row_range_idx_ = 0;
  all_in_memory_ = false;
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
    ObSEArray<ObDatumRowkey, OB_DEFAULT_MULTI_GET_ROWKEY_NUM> tmp_rowkeys;
    for (int64_t i = 0; i < rowkeys_->count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(tmp_rowkeys.push_back(rowkeys_->at(i)))) {
        STORAGE_LOG(WARN, "push back rowkey failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t l = curr_scan_index_ + 1;
      int64_t r = tmp_rowkeys.count();

      rowkeys_->reuse();
      range_idx_delta_ += curr_scan_index_ + 1;
      for (int64_t i = l; i < r && OB_SUCC(ret); ++i) {
        tmp_rowkeys.at(i).is_skip_prefetch_ = false;
        if (OB_FAIL(rowkeys_->push_back(tmp_rowkeys.at(i)))) {
          STORAGE_LOG(WARN, "push back rowkey failed", K(ret));
        }
      }
      STORAGE_LOG(DEBUG, "skip rowkeys", KPC(rowkeys_), K(range_idx_delta_));
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

int ObMultipleGetMerge::construct_iters()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tables_.count() == 0)) {
  } else {
    if (is_read_memtable_only()) {
      access_ctx_->use_fuse_row_cache_ = false;
      if (OB_FAIL(construct_specified_iters(is_memtable))) {
        STORAGE_LOG(WARN, "fail to construct iterators from all memtables only", K(ret));
      }
    } else {
      // firstly construct iterators for all memtables
      // then iterate OB_MULTI_GET_OPEN_ROWKEY_NUM rowkeys in memtables at most
      // finally construct iterators for all sstables
      // and prefetch OB_MULTI_GET_OPEN_ROWKEY_NUM rowkeys in sstables at most
      if (OB_FAIL(init_resource())) {
        STORAGE_LOG(WARN, "fail to init resource", K(ret));
      } else if (OB_FAIL(construct_specified_iters(is_memtable))) {
        STORAGE_LOG(WARN, "fail to construct iterators from all memtables", K(ret));
      } else if (OB_FAIL(get_rows_from_memory())) {
        STORAGE_LOG(WARN, "fail to get rows from memory", K(ret), K(prefetch_row_range_idx_));
      } else if (!all_in_memory_) {
        if (OB_FAIL(construct_specified_iters(is_sstable))) {
          STORAGE_LOG(WARN, "fail to construct iterators from all sstables", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMultipleGetMerge::inner_get_next_row(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (is_read_memtable_only()) {
    access_ctx_->use_fuse_row_cache_ = false;
    if (OB_FAIL(inner_get_next_row_for_memtables_only(row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next row in the scenario where only memtables exist", K(ret), K(get_row_range_idx_));
      }
    }
  } else if (OB_FAIL(inner_get_next_row_for_sstables_exist(row))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to get next row in the scenario where sstables exist", K(ret), K(get_row_range_idx_));
    }
  }
  return ret;
}

int ObMultipleGetMerge::alloc_resource()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr != full_rows_)) {
  } else {
    void *buf = nullptr;
    size_t buf_len = sizeof(ObQueryRowInfo) * common::OB_MULTI_GET_OPEN_ROWKEY_NUM;
    if (OB_ISNULL(buf = long_life_allocator_->alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate buffer", K(ret), K(buf_len));
    } else {
      full_rows_ = new(buf) ObQueryRowInfo[common::OB_MULTI_GET_OPEN_ROWKEY_NUM]();
    }
    STORAGE_LOG(DEBUG, "alloc memory", K(ret), K(buf_len));
  }
  return ret;
}

int ObMultipleGetMerge::init_resource()
{
  int ret = OB_SUCCESS;
  ObITable *table = tables_.at(0);
  const ObTabletMeta &tablet_meta = get_table_param_->tablet_iter_.get_tablet()->get_tablet_meta();
  access_ctx_->use_fuse_row_cache_ = access_ctx_->use_fuse_row_cache_ &&
                                    access_param_->iter_param_.enable_fuse_row_cache(access_ctx_->query_flag_) &&
                                    access_ctx_->trans_version_range_.snapshot_version_ >= tablet_meta.snapshot_version_ &&
                                    (!table->is_co_sstable() || static_cast<ObCOSSTableV2 *>(table)->is_all_cg_base()) &&
                                    OB_ISNULL(get_table_param_->tablet_iter_.get_split_extra_tablet_handles_ptr()) &&
                                    !(!tablet_meta.table_store_flag_.with_major_sstable() && tablet_meta.split_info_.get_split_src_tablet_id().is_valid()) && // not split dst tablet
                                    !tablet_meta.has_transfer_table() &&
                                    !is_fuse_row_cache_force_disable();
  access_ctx_->query_flag_.set_not_use_row_cache();
  STORAGE_LOG(DEBUG, "multiple get merge start", K(rowkeys_->count()), K(tables_.count()), K(iters_.count()), K(access_ctx_->use_fuse_row_cache_),
              K(access_param_->iter_param_.enable_fuse_row_cache(access_ctx_->query_flag_)),
              K(tablet_meta.snapshot_version_), K(access_ctx_->get_fuse_row_cache_put_count_threshold()));

  if (access_ctx_->use_fuse_row_cache_) {
    if (OB_FAIL(fuse_row_cache_fetcher_.init(access_ctx_->get_scan_type(),
                                             access_param_->iter_param_.tablet_id_,
                                             access_param_->iter_param_.get_read_info(),
                                             access_ctx_->trans_version_range_.base_version_,
                                             access_ctx_->trans_version_range_.snapshot_version_))) {
      STORAGE_LOG(WARN, "fail to init fuse row cache fetcher", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(alloc_resource())) {
    STORAGE_LOG(WARN, "fail to alloc resource", K(ret));
  } else {
    int64_t max_rowkey_cnt = MIN(rowkeys_->count(), common::OB_MULTI_GET_OPEN_ROWKEY_NUM);
    for (int64_t i = 0; OB_SUCC(ret) && i < max_rowkey_cnt; ++i) {
      cache_handles_[i].reset();
      ObQueryRowInfo &row_info = full_rows_[i];
      row_info.reuse();
      if (!row_info.row_.is_valid()) {
        if (OB_FAIL(row_info.row_.init(*long_life_allocator_, access_param_->get_max_out_col_cnt()))) {
          STORAGE_LOG(WARN, "fail to init datum row", K(ret), K(i), K(row_info));
        }
      } else if (OB_FAIL(row_info.row_.reserve(access_param_->get_max_out_col_cnt()))) {
        STORAGE_LOG(WARN, "fail to reserve full row", K(ret), K(i), K(row_info));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(row_info.nop_pos_.init(*long_life_allocator_, access_param_->get_max_out_col_cnt()))) {
        STORAGE_LOG(WARN, "fail to init nop_pos_", K(ret), K(i), K(row_info));
      } else {
        row_info.row_.count_ = 0;
        row_info.end_iter_idx_ = tables_.count();
      }
    }
  }
  return ret;
}

int ObMultipleGetMerge::construct_specified_iters(const CHECK_TABLE_TYPE check_table_type)
{
  int ret = OB_SUCCESS;
  ObStoreRowIterator *iter = nullptr;
  const ObTableIterParam *iter_param = nullptr;
  const int64_t iter_cnt = iters_.count();
  int64_t iter_idx = 0;

  for (int64_t i = tables_.count() - 1; OB_SUCC(ret) && i >= 0; --i, ++iter_idx) {
    ObITable *table = nullptr;
    if (OB_FAIL(tables_.at(i, table))) {
      STORAGE_LOG(WARN, "fail to get table", K(ret));
    } else if (check_table_type(table)) {
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
    } else if (is_sstable(table)) {
      break;
    }
  }
  STORAGE_LOG(DEBUG, "construct iterators success", K(ret), K(iter_cnt), K(iters_.count()));
  return ret;
}

int ObMultipleGetMerge::try_get_fuse_row_cache(
    const int64_t &read_snapshot_version,
    const int64_t &multi_version_start,
    ObDatumRowkey &rowkey,
    ObFuseRowValueHandle &handle,
    ObQueryRowInfo &row_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fuse_row_cache_fetcher_.get_fuse_row_cache(rowkey, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get value from fuse row cache", K(ret), K(rowkey));
    } else {
      ret = OB_SUCCESS;
      ++access_ctx_->table_store_stat_.fuse_row_cache_miss_cnt_;
      STORAGE_LOG(DEBUG, "fuse row cache miss", K(read_snapshot_version), K(multi_version_start), K(rowkey));
    }
  } else if (OB_UNLIKELY(handle.value_->get_read_snapshot_version() <= multi_version_start
                        || handle.value_->get_read_snapshot_version() > read_snapshot_version)) {
    STORAGE_LOG(DEBUG, "fuse row cache useless", K(handle), K(multi_version_start), K(read_snapshot_version), K(rowkey));
    handle.reset();
  } else {
    ObITable *table = nullptr;
    bool cover_partial_sstable = false;
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < tables_.count(); ++i) {
      if (OB_ISNULL(table = tables_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null table", K(ret), K(i), K(tables_));
      } else if (table->is_memtable()) {
        break;
      } else if ((table->is_major_sstable() && handle.value_->get_read_snapshot_version() < table->get_snapshot_version())
                  || (!table->is_major_sstable() && handle.value_->get_read_snapshot_version() < table->get_upper_trans_version())) {
        cover_partial_sstable = true;
        STORAGE_LOG(DEBUG, "sstable hit", K(rowkey), K(i), K(row_info), K(handle), K(table->get_upper_trans_version()));
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (cover_partial_sstable) {
      if (0 != i) {
        row_info.end_iter_idx_ = tables_.count() - i;
      } else {
        handle.reset();
      }
    } else {
      // cover all sstable
      row_info.end_iter_idx_ = 0;
      rowkey.is_skip_prefetch_ = true;
    }
  }
  return ret;
}

int ObMultipleGetMerge::iter_fuse_row_from_memtable(
    ObDatumRowkey &cur_rowkey,
    ObDatumRow &row,
    ObNopPos &nop_pos,
    bool &has_uncommitted_row)
{
  int ret = OB_SUCCESS;
  int64_t table_cnt = tables_.count();
  const ObDatumRow *tmp_row = nullptr;
  // TODO by xuanxi：
  // When the final_result of fuse row is true, end the iteration of the current rowkey;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_cnt; ++i) {
    ObITable *table = nullptr;
    if (OB_FAIL(tables_.at(table_cnt - i - 1, table))) {
      STORAGE_LOG(WARN, "fail to get table", K(ret));
    } else if (!table->is_memtable()) {
      break;
    } else if (OB_FAIL(iters_[i]->get_next_row(tmp_row))) {
      STORAGE_LOG(WARN, "iterator get next row failed", K(ret), K(i), K(cur_rowkey));
    } else if (cur_rowkey.is_skip_prefetch_) {
    } else if (OB_ISNULL(tmp_row)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "tmp_row is NULL", K(ret), K(i), K(cur_rowkey), K(row), K(nop_pos));
    } else {
      // fuse working row and result row
      if (OB_FAIL(ObRowFuse::fuse_row(*tmp_row, row, nop_pos, cur_rowkey.is_skip_prefetch_, nullptr))) {
        STORAGE_LOG(WARN, "fail to merge rows", K(ret), K(*tmp_row), K(row), K(nop_pos));
      } else if (access_ctx_->use_fuse_row_cache_ && tmp_row->row_flag_.is_exist() && !has_uncommitted_row) {
        has_uncommitted_row = tmp_row->is_have_uncommited_row() || row.snapshot_version_ == INT64_MAX;
      }
    }
  }
  STORAGE_LOG(DEBUG, "iterate memtables finished", K(ret), K(cur_rowkey), K(row), K(nop_pos), K(has_uncommitted_row), K(access_ctx_->use_fuse_row_cache_));
  return ret;
}

int ObMultipleGetMerge::iter_fuse_row_from_memtable(
    ObDatumRowkey &cur_rowkey,
    ObDatumRow &row,
    ObNopPos &nop_pos)
{
  int ret = OB_SUCCESS;
  bool has_uncommitted_row = false;
  if (OB_FAIL(iter_fuse_row_from_memtable(cur_rowkey, row, nop_pos, has_uncommitted_row))) {
    STORAGE_LOG(WARN, "fail to iterate row", K(ret), K(cur_rowkey), K(row), K(nop_pos));
  }
  return ret;
}

int ObMultipleGetMerge::iter_fuse_row_from_sstable(ObQueryRowInfo &row_info)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey &cur_rowkey = rowkeys_->at(get_row_range_idx_);
  const ObDatumRow *tmp_row = nullptr;
  // TODO by xuanxi：
  // When the final_result of fuse row is true, end the iteration of the current rowkey;
  for (int64_t i = 0; OB_SUCC(ret) && i < iters_.count(); ++i) {
    cur_rowkey.is_skip_prefetch_ = cur_rowkey.is_skip_prefetch_ ? true : (i >= row_info.end_iter_idx_);
    if (!iters_[i]->is_sstable_iter()) {
    } else if (OB_FAIL(iters_[i]->get_next_row(tmp_row))) {
      STORAGE_LOG(WARN, "iterator get next row failed", K(ret), K(i),  K(cur_rowkey));
    } else if (cur_rowkey.is_skip_prefetch_) {
    } else if (OB_ISNULL(tmp_row)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "tmp_row is NULL", K(ret), K(i),  K(cur_rowkey), K(row_info), K(access_ctx_->use_fuse_row_cache_));
    } else {
      // fuse working row and result row
      if (OB_FAIL(ObRowFuse::fuse_row(*tmp_row, row_info.row_, row_info.nop_pos_, cur_rowkey.is_skip_prefetch_))) {
        STORAGE_LOG(WARN, "fail to merge rows", K(ret), K(i), K(*tmp_row), K(cur_rowkey), K(row_info));
      } else if (access_ctx_->use_fuse_row_cache_ && tmp_row->row_flag_.is_exist() && !row_info.has_uncommitted_row_) {
        row_info.has_uncommitted_row_ = tmp_row->is_have_uncommited_row() || row_info.row_.snapshot_version_ == INT64_MAX;
      }
    }
  }
  STORAGE_LOG(DEBUG, "iterate sstables finished", K(ret), K(cur_rowkey), K(row_info), K(access_ctx_->use_fuse_row_cache_));
  return ret;
}

int ObMultipleGetMerge::fuse_cache_row(const ObFuseRowValueHandle &handle, ObQueryRowInfo &fuse_row_info)
{
  int ret = OB_SUCCESS;
  if (handle.is_valid()) {
    ObDatumRow cache_row;
    cache_row.count_ = handle.value_->get_column_cnt();
    cache_row.storage_datums_ = handle.value_->get_datums();
    cache_row.row_flag_ = handle.value_->get_flag();
    ++access_ctx_->table_store_stat_.fuse_row_cache_hit_cnt_;
    if (cache_row.row_flag_.is_exist()) {
      bool final_result = false;
      if (OB_FAIL(ObRowFuse::fuse_row(cache_row, fuse_row_info.row_, fuse_row_info.nop_pos_, final_result))) {
        STORAGE_LOG(WARN, "fail to fuse row", K(ret), K(fuse_row_info));
      } else {
        STORAGE_LOG(DEBUG, "fuse row cache success", K(cache_row), K(fuse_row_info));
      }
    }
  }
  return ret;
}

int ObMultipleGetMerge::check_final_row(ObDatumRow &fuse_row, bool &is_valid_row)
{
  int ret = OB_SUCCESS;
  is_valid_row = false;
  if (fuse_row.row_flag_.is_exist_without_delete() || (need_iter_del_row() && fuse_row.row_flag_.is_delete())) {
    // find result
    fuse_row.scan_index_ = get_row_range_idx_;
    fuse_row.group_idx_ = rowkeys_->at(get_row_range_idx_).get_group_idx();
    STORAGE_LOG(DEBUG, "get valid row", KP(this), K(rowkeys_->at(get_row_range_idx_)),
                                                  K(fuse_row), K(get_row_range_idx_), K(access_ctx_->use_fuse_row_cache_));
    is_valid_row = true;
  } else {
    // When the index lookups the rowkeys from the main table, it should exists
    // and if we find that it does not exist, there must be an anomaly
    if (GCONF.enable_defensive_check()
        && access_ctx_->query_flag_.is_lookup_for_4377()) {
      ret = handle_4377("[index lookup]ObMultipleGetMerge::inner_get_next_row");
      STORAGE_LOG(WARN,"[index lookup] row not found", K(ret),
                  K(access_ctx_->use_fuse_row_cache_),
                  KPC(rowkeys_),
                  K(get_row_range_idx_),
                  K(rowkeys_->count()),
                  K(rowkeys_->at(get_row_range_idx_)),
                  K(fuse_row));
    }
  }
  STORAGE_LOG(DEBUG, "check final row finished", KP(this), K(rowkeys_->at(get_row_range_idx_)), K(is_valid_row),
                                                 K(fuse_row), K(get_row_range_idx_), K(access_ctx_->use_fuse_row_cache_));
  return ret;
}

int ObMultipleGetMerge::project_final_row(const ObDatumRow &fuse_row, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const ObITableReadInfo *read_info = access_param_->iter_param_.get_read_info();
  const ObColumnIndexArray &cols_index = read_info->get_columns_index();
  row.count_ = read_info->get_request_count();
  const ObIArray<int32_t> *projector = (cols_index.rowkey_mode_ || !access_ctx_->use_fuse_row_cache_) ? nullptr : &cols_index.array_;
  STORAGE_LOG(DEBUG, "try to project row", K(ret), K(get_row_range_idx_), K(cols_index), K(access_ctx_->use_fuse_row_cache_),
                                          KPC(projector), K(rowkeys_->at(get_row_range_idx_)), K(fuse_row), K(row.count_));
  if (OB_FAIL(project_row(fuse_row, projector, 0/*range idx delta*/, row))) {
    STORAGE_LOG(WARN, "fail to project row", K(ret), K(fuse_row), K(cols_index));
  } else {
    row.trans_info_ = fuse_row.trans_info_;
  }
  STORAGE_LOG(DEBUG, "project final row finished", KP(this), K(rowkeys_->at(get_row_range_idx_)),
                                                   K(fuse_row), K(row), K(get_row_range_idx_), K(access_ctx_->use_fuse_row_cache_));
  return ret;
}

int ObMultipleGetMerge::get_rows_from_memory()
{
  int ret = OB_SUCCESS;
  int64_t max_prefetch_rowkey_cnt = MIN(rowkeys_->count(), common::OB_MULTI_GET_OPEN_ROWKEY_NUM);
  const int64_t &multi_version_start = get_table_param_->tablet_iter_.get_tablet()->get_tablet_meta().multi_version_start_;
  const int64_t &read_snapshot_version = access_ctx_->trans_version_range_.snapshot_version_;
  int64_t in_mem_cnt = 0;
  ObITable *table = tables_.at(tables_.count() - 1);
  if (table->is_memtable()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < max_prefetch_rowkey_cnt; ++i) {
      ObQueryRowInfo &row_info = full_rows_[i % common::OB_MULTI_GET_OPEN_ROWKEY_NUM];
      ObDatumRowkey &rowkey = rowkeys_->at(i);
      if (OB_FAIL(iter_fuse_row_from_memtable(rowkey, row_info.row_, row_info.nop_pos_, row_info.has_uncommitted_row_))) {
        STORAGE_LOG(WARN, "fail to iterate rows for memory tables", K(ret), K(rowkey), K(row_info));
      } else if (access_ctx_->use_fuse_row_cache_ && OB_FAIL(check_final_result(row_info.nop_pos_, rowkey.is_skip_prefetch_))) {
        STORAGE_LOG(WARN, "fail to check final result", K(ret), K(rowkey), K(row_info));
      } else if (rowkey.is_skip_prefetch_) {
        row_info.end_iter_idx_ = 0;
        ++in_mem_cnt;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (in_mem_cnt < max_prefetch_rowkey_cnt && access_ctx_->use_fuse_row_cache_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < max_prefetch_rowkey_cnt; ++i) {
      int64_t prefetch_idx = i % common::OB_MULTI_GET_OPEN_ROWKEY_NUM;
      ObQueryRowInfo &row_info = full_rows_[prefetch_idx];
      ObDatumRowkey &rowkey = rowkeys_->at(i);
      if (rowkey.is_skip_prefetch_) {
      } else if (OB_FAIL(try_get_fuse_row_cache(read_snapshot_version, multi_version_start, rowkey, cache_handles_[prefetch_idx], row_info))) {
        STORAGE_LOG(WARN, "fail to get fuse_row_cache", K(ret), K(access_ctx_->use_fuse_row_cache_), K(rowkey), K(row_info));
      } else if (rowkey.is_skip_prefetch_) {
        ++in_mem_cnt;
      }
    }
  }
  if (OB_SUCC(ret)) {
    prefetch_row_range_idx_ = max_prefetch_rowkey_cnt;
    all_in_memory_ = (rowkeys_->count() == in_mem_cnt);
  }
  return ret;
}

int ObMultipleGetMerge::prepare_prefetch_next_rowkey(
  const int64_t &multi_version_start,
  const int64_t &read_snapshot_version)
{
  int ret = OB_SUCCESS;
  ObITable *table = tables_.at(tables_.count() - 1);
  ObDatumRowkey &rowkey = rowkeys_->at(prefetch_row_range_idx_);
  int64_t prefetch_idx = prefetch_row_range_idx_ % common::OB_MULTI_GET_OPEN_ROWKEY_NUM;
  ObQueryRowInfo &row_info = full_rows_[prefetch_idx];
  ObFuseRowValueHandle &handle = cache_handles_[prefetch_idx];
  row_info.reuse();
  row_info.row_.count_ = 0;
  row_info.end_iter_idx_ = tables_.count();
  handle.reset();
  if (table->is_memtable()) {
    if (OB_FAIL(iter_fuse_row_from_memtable(rowkey, row_info.row_, row_info.nop_pos_, row_info.has_uncommitted_row_))) {
      STORAGE_LOG(WARN, "fail to iterate rows for memory tables", K(ret), K(rowkey), K(row_info));
    } else if (access_ctx_->use_fuse_row_cache_ && OB_FAIL(check_final_result(row_info.nop_pos_, rowkey.is_skip_prefetch_))) {
      STORAGE_LOG(WARN, "fail to check final result", K(ret), K(rowkey), K(row_info));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (rowkey.is_skip_prefetch_) {
    row_info.end_iter_idx_ = 0;
  } else if (access_ctx_->use_fuse_row_cache_) {
    if (OB_FAIL(try_get_fuse_row_cache(read_snapshot_version, multi_version_start, rowkey, handle, row_info))) {
      STORAGE_LOG(WARN, "fail to get fuse_row_cache", K(ret), K(access_ctx_->use_fuse_row_cache_), K(rowkey), K(row_info));
    }
  }
  STORAGE_LOG(DEBUG, "prepare to prefetch next rowkey", K(prefetch_row_range_idx_), K(rowkey), K(prefetch_idx),
                                                        K(row_info), K(handle), K(access_ctx_->use_fuse_row_cache_));
  return ret;
}

int ObMultipleGetMerge::try_get_next_row(ObQueryRowInfo &row_info, ObFuseRowValueHandle &handle)
{
  int ret = OB_SUCCESS;
  if (!row_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid row_info", K(ret), K(get_row_range_idx_), K(row_info), K(handle));
  } else if (!all_in_memory_ && OB_FAIL(iter_fuse_row_from_sstable(row_info))) {
    STORAGE_LOG(WARN, "fail to iterate sstables", K(ret), K(access_ctx_->use_fuse_row_cache_), K(tables_.count()), K(iters_.count()), K(get_row_range_idx_),
                                                  K(prefetch_row_range_idx_), K(rowkeys_->count()), K(rowkeys_->at(get_row_range_idx_)), K(row_info), K(handle));
  } else if (OB_FAIL(fuse_cache_row(handle, row_info))) {
    STORAGE_LOG(WARN, "fail to fuse cache row", K(ret), K(get_row_range_idx_), K(row_info), K(handle));
  } else {
    STORAGE_LOG(DEBUG, "get next row success", K(access_ctx_->use_fuse_row_cache_), K(tables_.count()), K(iters_.count()), K(get_row_range_idx_), K(all_in_memory_),
                                               K(prefetch_row_range_idx_), K(rowkeys_->count()), K(rowkeys_->at(get_row_range_idx_)), K(row_info), K(handle));
  }
  return ret;
}

int ObMultipleGetMerge::inner_get_next_row_for_memtables_only(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool is_valid_row = false;
  int64_t rowkey_cnt = rowkeys_->count();

  if (OB_UNLIKELY(0 == tables_.count())) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret) && !is_valid_row) {
      if (get_row_range_idx_ >= rowkey_cnt) {
        ret = OB_ITER_END;
      } else {
        ObDatumRow &fuse_row = row;
        fuse_row.count_ = 0;
        fuse_row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
        fuse_row.snapshot_version_ = 0L;
        nop_pos_.reset();

        if (OB_FAIL(iter_fuse_row_from_memtable(rowkeys_->at(get_row_range_idx_), fuse_row, nop_pos_))) {
          STORAGE_LOG(WARN, "fail to iterate rows for memory tables", K(ret), K(get_row_range_idx_), K(rowkeys_->at(get_row_range_idx_)), K(fuse_row), K(nop_pos_));
        } else if (OB_FAIL(check_final_row(fuse_row, is_valid_row))) {
          STORAGE_LOG(WARN, "fail to check final row validity", K(ret), K(fuse_row), K(is_valid_row));
        } else {
          ++get_row_range_idx_;
        }
      }
    }
  }
  return ret;
}

int ObMultipleGetMerge::inner_get_next_row_for_sstables_exist(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool is_valid_row = false;
  int64_t rowkey_cnt = rowkeys_->count();
  int64_t idx = 0;
  const int64_t &multi_version_start = get_table_param_->tablet_iter_.get_tablet()->get_tablet_meta().multi_version_start_;
  const int64_t &read_snapshot_version = access_ctx_->trans_version_range_.snapshot_version_;

  if (OB_UNLIKELY(0 == tables_.count())) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret) && !is_valid_row) {
      if (get_row_range_idx_ >= rowkey_cnt) {
        ret = OB_ITER_END;
      } else {
        idx = get_row_range_idx_ % common::OB_MULTI_GET_OPEN_ROWKEY_NUM;
        ObQueryRowInfo &row_info = full_rows_[idx];
        ObFuseRowValueHandle &handle = cache_handles_[idx];
        is_valid_row = false;

        if (OB_FAIL(try_get_next_row(row_info, handle))) {
          STORAGE_LOG(WARN, "fail to try get next row", K(ret), K(get_row_range_idx_), K(idx), K(row_info));
        } else if (OB_FAIL(check_final_row(row_info.row_, is_valid_row))) {
          STORAGE_LOG(WARN, "fail to check final row validity", K(ret), K(row_info), K(is_valid_row));
        } else if (is_valid_row && OB_FAIL(project_final_row(row_info.row_, row))) {
          STORAGE_LOG(WARN, "fail to check final row validity", K(ret), K(row_info), K(is_valid_row));
        } else if (access_ctx_->use_fuse_row_cache_ && row_info.need_update_cache(access_ctx_->enable_put_fuse_row_cache())) {
          // try to put row cache
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = fuse_row_cache_fetcher_.put_fuse_row_cache(rowkeys_->at(get_row_range_idx_), row_info.row_))) {
            STORAGE_LOG(WARN, "fail to put fuse row cache", K(ret), K(rowkeys_->at(get_row_range_idx_)), K(row_info), K(row));
          } else {
            access_ctx_->table_store_stat_.fuse_row_cache_put_cnt_++;
          }
        }
        // prepare to prefetch next rowkey
        if (OB_FAIL(ret)) {
        } else if (FALSE_IT(++get_row_range_idx_)) {
        } else if (prefetch_row_range_idx_ < rowkey_cnt) {
          if (OB_FAIL(prepare_prefetch_next_rowkey(multi_version_start, read_snapshot_version))) {
            STORAGE_LOG(WARN, "fail to prepare to prefetch next rowkey", K(ret), K(prefetch_row_range_idx_), K(rowkey_cnt));
          } else {
            ++prefetch_row_range_idx_;
          }
        }
      }
    }
  }
  return ret;
}

}
}
