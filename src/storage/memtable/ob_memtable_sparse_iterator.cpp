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

#define USING_LOG_PREFIX TRANS

#include "storage/memtable/ob_memtable_sparse_iterator.h"
#include "storage/memtable/ob_memtable_row_reader.h"
#include "ob_memtable.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace transaction;
using namespace share::schema;
namespace memtable {

/**
 * ---------------------------ObMemtableMultiVersionScanSparseIterator----------------------------
 */

ObMemtableMultiVersionScanSparseIterator::ObMemtableMultiVersionScanSparseIterator()
{}

ObMemtableMultiVersionScanSparseIterator::~ObMemtableMultiVersionScanSparseIterator()
{}

int ObMemtableMultiVersionScanSparseIterator::init_row_cells(ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "allocator is null");
  } else if (NULL == (row_.row_val_.cells_ = (ObObj*)allocator->alloc(sizeof(ObObj) * OB_ROW_MAX_COLUMNS_COUNT))) {
    TRANS_LOG(WARN, "arena alloc cells fail", "size", sizeof(ObObj) * OB_ROW_MAX_COLUMNS_COUNT);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (NULL == (row_.column_ids_ = (uint16_t*)allocator->alloc(sizeof(uint16_t) * OB_ROW_MAX_COLUMNS_COUNT))) {
    TRANS_LOG(WARN, "arena alloc column id fail", "size", sizeof(uint16_t) * OB_ROW_MAX_COLUMNS_COUNT);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    row_.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
    row_.row_val_.count_ = 0;
    row_.is_sparse_row_ = true;
  }
  return ret;
}

int ObMemtableMultiVersionScanSparseIterator::init_next_value_iter()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_row()) || OB_ISNULL(key_) || OB_ISNULL(value_iter_)) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "row_iter_ get_next_row fail", K(ret), KP(key_), KP(value_iter_));
    }
    ret = (OB_SUCCESS == ret) ? OB_ERR_UNEXPECTED : ret;
  } else {
    row_.set_first_dml(value_iter_->get_first_dml());
    value_iter_->set_merge_log_ts(context_->merge_log_ts_);
    value_iter_->set_iter_mode(iter_mode_);
    row_checker_.reset();
  }
  return ret;
}

void ObMemtableMultiVersionScanSparseIterator::row_reset()
{
  row_state_reset();
  row_.row_val_.count_ = 0;
}

int ObMemtableMultiVersionScanSparseIterator::iterate_multi_version_row(
    const ObStoreRowkey& key, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReadSparseRow::prepare_sparse_rowkey_position(key.get_obj_cnt(), row))) {
    TRANS_LOG(WARN, "prepare rowkey position fail", K(ret), K(key));
  } else if (OB_FAIL(iterate_multi_version_row_value_(row))) {
    TRANS_LOG(WARN, "iterate_multi_version_row_value_ fail", K(ret), K(key));
  } else if (OB_FAIL(ObReadSparseRow::iterate_sparse_row_key(columns_, key, row))) {
    TRANS_LOG(WARN, "iterate_row_key_ fail", K(ret), K(key));
  }
  TRANS_LOG(DEBUG, "iterate_multi_version_sparse_row success", K(ret), K(row), K(key));
  return ret;
}

int ObMemtableMultiVersionScanSparseIterator::iterate_compacted_row(const ObStoreRowkey& key, ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReadSparseRow::prepare_sparse_rowkey_position(key.get_obj_cnt(), row))) {
    TRANS_LOG(WARN, "prepare rowkey position fail", K(ret), K(key));
  } else if (OB_FAIL(iterate_compacted_row_value_(row))) {
    TRANS_LOG(WARN, "iterate_row_value fail", K(ret), K(key));
  } else if (OB_FAIL(ObReadSparseRow::iterate_sparse_row_key(columns_, key, row))) {
    TRANS_LOG(WARN, "iterate_row_key_ fail", K(ret), K(key));
  }
  TRANS_LOG(DEBUG, "iterate_compacted_sparse_row success", K(ret), K(row), K(key));
  return ret;
}

int ObMemtableMultiVersionScanSparseIterator::iterate_uncommitted_row(
    const ObStoreRowkey& key, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObReadSparseRow::prepare_sparse_rowkey_position(key.get_obj_cnt(), row))) {
    TRANS_LOG(WARN, "prepare rowkey position fail", K(ret), K(key));
  } else if (OB_FAIL(iterate_uncommitted_row_value_(row))) {
    TRANS_LOG(WARN, "iterate_uncommitted_row_value_ fail", K(ret), K(key));
  } else if (OB_FAIL(ObReadSparseRow::iterate_sparse_row_key(columns_, key, row))) {
    TRANS_LOG(WARN, "iterate_row_key_ fail", K(ret), K(key));
  }
  TRANS_LOG(DEBUG, "iterate_uncommitted_row success", K(ret), K(row), K(key));
  return ret;
}

OB_INLINE int ObReadSparseRow::iterate_sparse_row_key(
    const ObIArray<ObColDesc>& columns, const ObStoreRowkey& rowkey, ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  const ObObj* obj_ptr = rowkey.get_obj_ptr();
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
    if (row.row_val_.cells_[i].is_nop_value()) {
      row.row_val_.cells_[i] = obj_ptr[i];
      row.column_ids_[i] = columns.at(i).col_id_;  // set column id
    }
    TRANS_LOG(
        DEBUG, "iterate_sparse_row_key", K(ret), K(i), K(row.column_ids_[i]), K(row.row_val_.cells_[i]), K(obj_ptr[i]));
  }
  return ret;
}

OB_INLINE int ObReadSparseRow::prepare_sparse_rowkey_position(const int64_t rowkey_length, ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (rowkey_length < 0 || OB_ISNULL(row.column_ids_) || row.capacity_ <= rowkey_length) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(rowkey_length), K(row.capacity_), K(row.column_ids_));
  } else {
    int index = 0;
    for (; index < rowkey_length; ++index) {  // set all rowkey nop
      row.row_val_.cells_[index].set_nop_value();
      row.column_ids_[index] = -1;
    }
    for (int i = 0; i < ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt(); ++i) {
      row.row_val_.cells_[index].set_nop_value();
      row.column_ids_[index] = OB_MULTI_VERSION_EXTRA_ROWKEY[i].column_index_;
      index++;
    }
    row.row_val_.count_ = index;
  }
  return ret;
}

}  // namespace memtable
}  // namespace oceanbase
