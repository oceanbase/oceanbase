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

#include "ob_value_row_iterator.h"

namespace oceanbase {
using namespace oceanbase::common;
namespace storage {

ObValueRowIterator::ObValueRowIterator()
    : ObNewRowIterator(),
      is_inited_(false),
      unique_(false),
      allocator_(ObModIds::OB_VALUE_ROW_ITER),
      rows_(),
      cur_idx_(0)
{}

ObValueRowIterator::~ObValueRowIterator()
{}

int ObValueRowIterator::init(bool unique)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObValueRowIterator is already initialized", K(ret));
  } else {
    is_inited_ = true;
    unique_ = unique;
    cur_idx_ = 0;
  }
  return ret;
}

int ObValueRowIterator::add_row(common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  ObNewRow* cur_row = NULL;
  if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row", K(ret), K(row));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObValueRowIterator is not initialized", K(ret));
  } else {
    bool exist = false;
    // check whether exists
    if (unique_ && rows_.count() > 0) {
      // we consider that in general, the probability that a row produces different conflicting rows
      // on multiple unique index is small, so there is usually only one row in the value row iterator
      // so using list traversal to deduplicate unique index is more efficiently
      // and also saves the CPU overhead that constructs the hash map
      ObStoreRowkey rowkey(row.cells_, row.count_);
      for (int64_t i = 0; OB_SUCC(ret) && !exist && i < rows_.count(); ++i) {
        ObStoreRowkey tmp_rowkey(rows_.at(i).cells_, rows_.at(i).count_);
        if (OB_UNLIKELY(tmp_rowkey == rowkey)) {
          exist = true;
        }
      }
    }
    // store non-exist row
    if (OB_SUCC(ret)) {
      if (!exist) {
        if (NULL == (cur_row = rows_.alloc_place_holder())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(ERROR, "add row error", K(ret));
        } else if (OB_SUCCESS != (ret = ob_write_row(allocator_, row, *cur_row))) {
          STORAGE_LOG(WARN, "copy row error", K(ret), K(row));
        }
      }
    }
  }
  return ret;
}

int ObValueRowIterator::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObValueRowIterator is not initialized", K(ret));
  } else if (cur_idx_ < rows_.count()) {
    row = &rows_.at(cur_idx_++);
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObValueRowIterator::get_next_rows(ObNewRow*& rows, int64_t& row_count)
{
  int ret = OB_SUCCESS;
  if (cur_idx_ < rows_.count()) {
    rows = &(rows_.at(cur_idx_));
    row_count = rows_.count() - cur_idx_;
    cur_idx_ = rows_.count();
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

void ObValueRowIterator::reset()
{
  rows_.reset();
  allocator_.reset();
  is_inited_ = false;
  unique_ = false;
  cur_idx_ = 0;
}

}  // end namespace storage
}  // end namespace oceanbase
