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

#include "ob_rows_info.h"
#include "storage/ob_relative_table.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_datum_row_utils.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

ObRowsInfo::ExistHelper::ExistHelper()
  : table_iter_param_(),
    table_access_context_(),
    is_inited_(false)
{
}

ObRowsInfo::ExistHelper::~ExistHelper()
{
}

void ObRowsInfo::ExistHelper::reset()
{
  table_iter_param_.reset();
  table_access_context_.reset();
  is_inited_ = false;
}

int ObRowsInfo::ExistHelper::init(const ObRelativeTable &table,
                                  ObStoreCtx &store_ctx,
                                  const ObITableReadInfo &rowkey_read_info,
                                  ObStorageReserveAllocator &stmt_allocator,
                                  ObStorageReserveAllocator &allocator,
                                  ObTruncatePartitionFilter *truncate_part_filter)
{
  int ret = OB_SUCCESS;
  const ObTablet *tablet = nullptr;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObExistPrefixScanHelper init twice", K(ret));
  } else {
    common::ObQueryFlag query_flag;
    common::ObVersionRange trans_version_range;
    memtable::ObMvccMdsFilter mds_filter;
    query_flag.read_latest_ = ObQueryFlag::OBSF_MASK_READ_LATEST;
    query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
    if (table.is_storage_index_table()) {
      query_flag.index_invalid_ = !table.can_read_index();
    }

    trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;

    mds_filter.truncate_part_filter_ = truncate_part_filter;
    mds_filter.read_info_ = &rowkey_read_info;

    if (OB_FAIL(table_access_context_.init(query_flag, store_ctx, allocator, stmt_allocator,
            trans_version_range, &mds_filter, true /*+ for_exist */))) {
      STORAGE_LOG(WARN, "failed to init table access ctx", K(ret));
    } else {
      table_iter_param_.table_id_ = table.get_table_id();
      table_iter_param_.tablet_id_ = table.get_tablet_id();
      if (nullptr != table.tablet_iter_.get_tablet()) {
        table_iter_param_.ls_id_ = table.tablet_iter_.get_tablet()->get_tablet_meta().ls_id_;
      }
      table_iter_param_.out_cols_project_ = NULL;
      table_iter_param_.read_info_ = &rowkey_read_info;
      table_iter_param_.set_tablet_handle(table.get_tablet_handle());
      is_inited_ = true;
    }
  }
  return ret;
}

ObRowsInfo::ObRowsInfo()
  : scan_mem_allocator_(ObMemAttr(MTL_ID(), common::ObModIds::OB_STORE_ROW_EXISTER), OB_MALLOC_NORMAL_BLOCK_SIZE),
    exist_allocator_(ObMemAttr(MTL_ID(), common::ObModIds::OB_STORE_ROW_EXISTER), OB_MALLOC_NORMAL_BLOCK_SIZE),
    key_allocator_(ObMemAttr(MTL_ID(), common::ObModIds::OB_STORE_ROW_EXISTER), OB_MALLOC_NORMAL_BLOCK_SIZE),
    rowkeys_(),
    permutation_(),
    rows_(nullptr),
    exist_helper_(),
    tablet_id_(),
    dup_row_iter_(nullptr),
    dup_row_column_ids_(nullptr),
    datum_utils_(nullptr),
    col_descs_(nullptr),
    conflict_rowkey_idx_(-1),
    error_code_(OB_SUCCESS),
    delete_count_(0),
    rowkey_column_num_(0),
    is_inited_(false),
    need_find_all_duplicate_key_(false),
    is_sorted_(false)
{
}

ObRowsInfo::~ObRowsInfo()
{
}

void ObRowsInfo::reset()
{
  exist_helper_.reset();
  rows_ = nullptr;
  delete_count_ = 0;
  rowkey_column_num_ = 0;
  datum_utils_ = nullptr;
  tablet_id_.reset();
  permutation_.reset();
  rowkeys_.reset();
  scan_mem_allocator_.reset();
  key_allocator_.reset();
  error_code_ = OB_SUCCESS;
  conflict_rowkey_idx_ = -1;
  is_sorted_ = false;
  is_inited_ = false;
  col_descs_ = nullptr;
}

int ObRowsInfo::init(
    const ObColDescIArray &column_descs,
    const ObRelativeTable &table,
    ObStoreCtx &store_ctx,
    const ObITableReadInfo &rowkey_read_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObRowsinfo init twice", K(ret));
  } else if (OB_FAIL(exist_helper_.init(table, store_ctx, rowkey_read_info, exist_allocator_, scan_mem_allocator_, table.get_truncate_part_filter()))) {
    STORAGE_LOG(WARN, "Failed to init exist helper", K(ret));
  } else {
    col_descs_ = &column_descs;
    datum_utils_ = &rowkey_read_info.get_datum_utils();
    tablet_id_ = table.get_tablet_id();
    rowkey_column_num_ = table.get_rowkey_column_num();
    is_inited_ = true;
  }
  return ret;
}

void ObRowsInfo::reuse()
{
  is_sorted_ = false;
  rows_ = nullptr;
  delete_count_ = 0;
  error_code_ = OB_SUCCESS;;
  conflict_rowkey_idx_ = -1;
  scan_mem_allocator_.reuse();
  rowkeys_.reuse();
  key_allocator_.reuse();
  need_find_all_duplicate_key_ = false;
}

int ObRowsInfo::assign_rows(const int64_t row_count, blocksstable::ObDatumRow *rows)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObRowsInfo not init", K(ret));
  } else if (OB_ISNULL(rows) || row_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid parameter", K(rows), K(row_count), K(ret));
  } else {
    reuse();
    rows_ = rows;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(rowkeys_.reserve(row_count))) {
      STORAGE_LOG(WARN, "Failed to reserve rowkeys", K(ret), K(row_count));
    } else if (OB_FAIL(permutation_.prepare_allocate(row_count))) {
      STORAGE_LOG(WARN, "Failed to prepare allocate permutation", K(ret), K(row_count));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      if (OB_UNLIKELY(!rows[i].is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid argument", K(rows_[i]), K(ret));
      } else {
        ObMarkedRowkeyAndLockState marked_rowkey_and_lock_state;
        marked_rowkey_and_lock_state.row_idx_ = i;
        ObDatumRowkey &datum_rowkey = marked_rowkey_and_lock_state.marked_rowkey_.get_rowkey();
        if (OB_FAIL(blocksstable::ObDatumRowUtils::prepare_rowkey(rows[i], rowkey_column_num_, *col_descs_, key_allocator_, datum_rowkey))) {
          STORAGE_LOG(WARN, "Failed to prepare rowkey", K(ret), K(rowkey_column_num_), K(rows_[i]));
        } else if (OB_FAIL(rowkeys_.push_back(marked_rowkey_and_lock_state))) {
          STORAGE_LOG(WARN, "Failed to push back rowkey", K(ret), K(marked_rowkey_and_lock_state));
        } else {
          permutation_[i] = i;
        }
      }
    }
    if (OB_SUCC(ret) && 1 == row_count) {
      is_sorted_ = true;
    }
  }
  return ret;
}

int ObRowsInfo::assign_duplicate_splitted_rows_info(
    const bool is_first_splitted_rows_info,
    ObIArray<int64_t> &row_idxs, // row_idx of rows_ in origin rows_info
    ObRowsInfo &splitted_rows_info)
{
  int ret = OB_SUCCESS;
  const int64_t origin_row_cnt = get_rowkey_cnt();
  const int64_t splitted_row_cnt = splitted_rows_info.get_rowkey_cnt();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObRowsInfo not init", K(ret));
  } else if (OB_UNLIKELY(splitted_rows_info.get_error_code() != OB_ERR_PRIMARY_KEY_DUPLICATE ||
      row_idxs.count() != splitted_row_cnt || origin_row_cnt < splitted_row_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(row_idxs.count()), K(origin_row_cnt), K(splitted_rows_info), K(ret));
  } else {
    // rows_info needs to be reset to a non-sorted state
    if (is_first_splitted_rows_info && is_sorted_) {
      for (int64_t i = 0; i < origin_row_cnt; i++) {
        permutation_[i] = i;
        rowkeys_.at(i).row_idx_ = i;
      }
      is_sorted_ = false;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < splitted_row_cnt; i++) {
      if (splitted_rows_info.rowkeys_.at(i).marked_rowkey_.is_row_duplicate()) {
        const int64_t origin_row_idx = row_idxs.at(splitted_rows_info.rowkeys_.at(i).row_idx_);
        rowkeys_.at(origin_row_idx) = splitted_rows_info.rowkeys_.at(i);
        // obj_ptr of store_rowkey is allocated by allocator in tmp_rows_info, so it need to be reset, 
        // otherwise later printting store_rowkey will core.
        rowkeys_.at(origin_row_idx).marked_rowkey_.get_rowkey().store_rowkey_.reset();
        rowkeys_.at(origin_row_idx).row_idx_ = origin_row_idx;
        set_row_conflict_error(origin_row_idx, OB_ERR_PRIMARY_KEY_DUPLICATE);
        STORAGE_LOG(DEBUG, "assign duplicate rowkey", K(i), K(splitted_rows_info.rowkeys_.at(i)), K(origin_row_idx), K(rowkeys_.at(origin_row_idx)));
      }
    }
  }
  return ret;
}

int ObRowsInfo::set_need_find_all_duplicate_rows(
    const bool need_find_all_duplicate_key,
    const common::ObIArray<uint64_t> *dup_row_column_ids,
    blocksstable::ObDatumRowIterator **dup_row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObRowsInfo not init", K(ret));
  } else if (OB_UNLIKELY(need_find_all_duplicate_key && (nullptr == dup_row_column_ids || nullptr == dup_row_iter))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(dup_row_column_ids), KP(dup_row_column_ids));
  } else {
    need_find_all_duplicate_key_ = need_find_all_duplicate_key;
    dup_row_column_ids_ = dup_row_column_ids;
    dup_row_iter_ = dup_row_iter;
  }
  return ret;
}

int ObRowsInfo::sort_keys()
{
  int ret = OB_SUCCESS;
  const int64_t row_count = rowkeys_.count();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObRowsInfo not init", K(ret));
  } else if (OB_UNLIKELY(0 == row_count)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row count error", K(ret));
  } else if (is_sorted_) {
    // has sorted, do nothing
  } else {
    blocksstable::ObDatumRowkey not_used_dup_key;
    RowsCompare rows_cmp(*datum_utils_, not_used_dup_key, false/*check_dup*/, ret);
    lib::ob_sort(rowkeys_.begin(), rowkeys_.end(), rows_cmp);
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < row_count; i++) {
        permutation_[rowkeys_[i].row_idx_] = i;
      }
      is_sorted_ = true;
      if (conflict_rowkey_idx_ != -1) {
        conflict_rowkey_idx_ = permutation_[conflict_rowkey_idx_];
      }
    }
  }
  return ret;
}

int ObRowsInfo::check_min_rowkey_boundary(const blocksstable::ObDatumRowkey &max_rowkey, bool &may_exist)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  may_exist = true;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected not init rowsinfo", K_(delete_count), KP_(rows), K(ret));
  } else if (OB_UNLIKELY(!max_rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to check min rowkey", K(ret), K(max_rowkey));
  } else if (OB_UNLIKELY(!is_sorted_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "rows must be has sorted", K(ret));
  } else if (OB_FAIL(rowkeys_.at(0).marked_rowkey_.get_rowkey().compare(max_rowkey, *datum_utils_, cmp_ret))) {
    STORAGE_LOG(WARN, "Failed to compare datum rowkey", K(ret), K(max_rowkey));
  } else if (cmp_ret > 0) {
    may_exist = false;
  }
  return ret;
}

int ObRowsInfo::refine_rowkeys()
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected not init rowsinfo", K_(delete_count), KP_(rows), K(ret));
  } else if (OB_FAIL(exist_helper_.table_access_context_.alloc_iter_pool(false))) {
    STORAGE_LOG(WARN, "Failed to alloc exist iter pool", K(ret));
  } else {
    for (int64_t i = 0; i < rowkeys_.count(); ++i) {
      rowkeys_[i].marked_rowkey_.clear_row_non_existent();
    }
  }
  return ret;
}

void ObRowsInfo::return_exist_iter(ObStoreRowIterator *exist_iter)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "Unexpected not init rowsinfo", K_(delete_count), KP_(rows), K(ret));
  } else if (OB_LIKELY(nullptr != exist_iter)) {
    exist_iter->reuse();
    if (exist_helper_.table_access_context_.get_stmt_iter_pool() != nullptr) {
      exist_helper_.table_access_context_.get_stmt_iter_pool()->return_iter(exist_iter);
    } else {
      exist_iter->~ObStoreRowIterator();
      exist_helper_.table_access_context_.stmt_allocator_->free(exist_iter);
    }
  }
}

} // namespace storage
} // namespace oceanbase
