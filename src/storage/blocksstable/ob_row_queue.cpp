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
#include "storage/ob_row_fuse.h"

namespace oceanbase
{
using namespace storage;

namespace blocksstable
{

int ObRowQueue::init(const int64_t col_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_UNLIKELY(0 >= col_cnt || col_cnt >= OB_ROW_MAX_COLUMNS_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(col_cnt));
  } else {
    col_cnt_ = col_cnt;
    cur_pos_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObRowQueue::add_empty_row(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDatumRow *row = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(alloc_row(row, allocator))) {
    STORAGE_LOG(WARN, "failed to alloc row", K(ret));
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row is NULL", K(ret));
  } else {
    row->count_ = 0;
    row->row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    row->mvcc_row_flag_.reset();
    if (OB_FAIL(rows_.push_back(row))) {
      STORAGE_LOG(WARN, "failed to push back", K(ret));
    }
    STORAGE_LOG(DEBUG, "add empty row", K(ret), KPC(row));
  }
  return ret;
}

int ObRowQueue::add_row(const ObDatumRow &src, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDatumRow *dest = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(alloc_row(dest, allocator))) {
    STORAGE_LOG(WARN, "failed to alloc row", K(ret));
  } else if (OB_ISNULL(dest)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row is NULL", K(ret));
  } else if (OB_FAIL(dest->deep_copy(src, allocator))) {
    STORAGE_LOG(WARN, "Failed to deep copy datum row", K(ret));
  } else if (OB_FAIL(rows_.push_back(dest))) {
    STORAGE_LOG(WARN, "failed to push back", K(ret));
  }
  return ret;
}

int ObRowQueue::get_next_row(const ObDatumRow *&row)
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

int ObRowQueue::alloc_row(ObDatumRow *&row, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    void *buf = NULL;
    int64_t malloc_column_count = col_cnt_;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObDatumRow)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc store row", K(ret));
    } else {
      row = new (buf) ObDatumRow;
      if (OB_FAIL(row->init(allocator, malloc_column_count))) {
        STORAGE_LOG(WARN, "Failed to init datum row", K(ret));
      }
    }
  }
  return ret;
}

int ObRowQueue::compact_border_row(const ObDatumRow *row,
                                   const bool last_row,
                                   storage::ObNopPos *&nop_pos,
                                   common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == row || nullptr == nop_pos)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("Unexpceted row or nop_pos", K(ret), KP(row), KP(nop_pos));
  } else if (row->is_ghost_row()) {
    // ghost row no need to compact
  } else {
    bool final_result = false;
    ObDatumRow *border_row = last_row ? get_last() : get_first();
    if (OB_ISNULL(border_row)) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("Unexpected null border row", K(ret), K(last_row), KPC(this));
    } else if (border_row->is_compacted_multi_version_row()) {
      // border row has already been compacted
    } else if (OB_FAIL(storage::ObRowFuse::fuse_row(
                *row, *border_row, *nop_pos, final_result, &allocator))) {
      STORAGE_LOG(WARN, "Failed to fuse row", K(ret));
    } else if (final_result) {
      border_row->set_compacted_multi_version_row();
    }
    if (OB_SUCC(ret) && !last_row) {
      // fuse flag to first row
      border_row->row_flag_.fuse_flag(row->row_flag_);
    }
    LOG_DEBUG("try to compact border row", K(ret), K(last_row), KPC(row), KPC(border_row));
  }
  return ret;
}

int ObRowQueue::add_shadow_row(
    const int64_t trans_seq_idx,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDatumRow *first_row = get_first();
  if (!first_row->is_shadow_row()) {
    if (OB_UNLIKELY(1 != count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected row queue", K(ret), K(count()), KPC(first_row), KPC(this));
    } else if (OB_FAIL(add_row(*first_row, allocator))) {
      LOG_WARN("failed to add row queue", K(ret), KPC(first_row), KPC(this));
    } else if (OB_FAIL(ObShadowRowUtil::make_shadow_row(trans_seq_idx, *first_row))) {
      LOG_WARN("failed to make shadow row", K(ret), KPC(first_row), K(trans_seq_idx));
    }
  }
  return ret;
}

}
}
