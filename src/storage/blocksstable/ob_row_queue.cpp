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

#include "ob_row_queue.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
using namespace storage;

namespace blocksstable {

int ObRowQueue::init(const ObRowStoreType row_type, const int64_t cell_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(0 >= cell_cnt) || OB_UNLIKELY(row_type >= MAX_ROW_STORE) ||
             (SPARSE_ROW_STORE == row_type_ && !GCONF._enable_sparse_row)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(cell_cnt), K(row_type));
  } else {
    cell_cnt_ = cell_cnt;
    cur_pos_ = 0;
    row_type_ = row_type;
    is_inited_ = true;
  }
  return ret;
}

int ObRowQueue::add_empty_row(ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObStoreRow* row = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(alloc_row(row, allocator))) {
    STORAGE_LOG(WARN, "failed to alloc row", K(ret));
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row is NULL", K(ret));
  } else {
    row->row_val_.count_ = 0;
    row->capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
    row->flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
    row->from_base_ = false;
    row->row_type_flag_.reset();
    if (OB_FAIL(rows_.push_back(row))) {
      STORAGE_LOG(WARN, "failed to push back", K(ret));
    }
    STORAGE_LOG(DEBUG, "add empty row", K(ret), KPC(row));
  }
  return ret;
}

int ObRowQueue::add_row(const ObStoreRow& src, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  ObStoreRow* dest = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(alloc_row(dest, allocator))) {
    STORAGE_LOG(WARN, "failed to alloc row", K(ret));
  } else if (OB_ISNULL(dest)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row is NULL", K(ret));
  } else if (OB_FAIL(deep_copy_row(*dest, src, allocator))) {
    STORAGE_LOG(WARN, "failed to deep copy objs", K(ret));
  } else if (OB_FAIL(rows_.push_back(dest))) {
    STORAGE_LOG(WARN, "failed to push back", K(ret));
  }
  return ret;
}

int ObRowQueue::get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (cur_pos_ >= rows_.count()) {
    ret = OB_ITER_END;
  } else {
    row = rows_.at(cur_pos_);
    ++cur_pos_;
  }
  return ret;
}

int ObRowQueue::alloc_row(ObStoreRow*& row, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    void* buf = NULL;
    int64_t malloc_column_count = OB_ROW_MAX_COLUMNS_COUNT;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStoreRow)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc store row", K(ret));
    } else {
      row = new (buf) ObStoreRow;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * malloc_column_count))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc cells", K(ret));
      } else {  // alloc cell array
        MEMSET(buf, 0, sizeof(ObObj) * malloc_column_count);
        row->row_val_.cells_ = new (buf) ObObj[malloc_column_count];
      }
      // alloc column id array
      if (OB_SUCC(ret) && SPARSE_ROW_STORE == row_type_) {
        if (OB_ISNULL(buf = allocator.alloc(sizeof(uint16_t) * malloc_column_count))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "alloc column ids failed", K(ret), K(malloc_column_count));
        } else {
          row->column_ids_ = (uint16_t*)buf;
        }
      }
      if (OB_SUCC(ret)) {
        row->is_sparse_row_ = (SPARSE_ROW_STORE == row_type_);
        row->row_val_.count_ = malloc_column_count;
        row->capacity_ = malloc_column_count;
        row->row_val_.projector_ = NULL;
        row->row_val_.projector_size_ = 0;
      }
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
