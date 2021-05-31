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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/sort/ob_in_memory_topn_sort.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/row/ob_row_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObInMemoryTopnSort::ObInMemoryTopnSort()
    : ObBaseSort(),
      topn_sort_array_pos_(0),
      is_fetch_with_ties_(false),
      iter_end_(false),
      last_row_(NULL),
      cmp_(NULL),
      heap_(cmp_, cur_alloc_)
{}

ObInMemoryTopnSort::~ObInMemoryTopnSort()
{
  reset();
}

void ObInMemoryTopnSort::reset()
{
  ObBaseSort::reset();
  heap_.reset();
  last_row_ = NULL;
  topn_sort_array_pos_ = 0;
  is_fetch_with_ties_ = false;
  iter_end_ = false;
}

void ObInMemoryTopnSort::reuse()
{
  ObBaseSort::reuse();
  heap_.reset();
  last_row_ = NULL;
  topn_sort_array_pos_ = 0;
  is_fetch_with_ties_ = false;
  iter_end_ = false;
}

ObInMemoryTopnSort::RowComparer::RowComparer(const ObIArray<ObSortColumn>* sort_columns) : sort_columns_(sort_columns)
{}

bool ObInMemoryTopnSort::RowComparer::operator()(const RowWrapper* r1, const RowWrapper* r2)
{
  bool ret = false;
  if (OB_ISNULL(r1) || OB_ISNULL(r2) || OB_ISNULL(sort_columns_)) {
    LOG_WARN("stored row or sort_columns_ is null", K(r1), K(r2), K(sort_columns_));
    common::right_to_die_or_duty_to_live();
  } else {
    ret = operator()(r1->row_, r2->row_);
  }
  return ret;
}

bool ObInMemoryTopnSort::RowComparer::operator()(const ObNewRow& r1, const ObNewRow& r2)
{
  bool ret = false;
  int cmp = 0;
  if (OB_ISNULL(sort_columns_)) {
    LOG_WARN("sort_columns_ is null", K(sort_columns_));
    common::right_to_die_or_duty_to_live();
  } else {
    for (int32_t i = 0; 0 == cmp && i < sort_columns_->count(); ++i) {
      int64_t idx = sort_columns_->at(i).index_;
      cmp = r1.get_cell(idx).compare(r2.get_cell(idx), sort_columns_->at(i).cs_type_);
      if (cmp < 0) {
        ret = sort_columns_->at(i).is_ascending();
      } else if (cmp > 0) {
        ret = !sort_columns_->at(i).is_ascending();
      } else {
      }
    }  // end for
  }
  return ret;
}

int ObInMemoryTopnSort::add_row(const ObNewRow& row, bool& need_sort)
{
  int ret = OB_SUCCESS;
  bool is_cur_block_row = true;
  if (OB_FAIL(check_block_row(row, last_row_, is_cur_block_row))) {
    LOG_WARN("check if row is cur block failed", K(ret));
  } else if (!is_cur_block_row && get_row_count() >= topn_cnt_) {
    need_sort = true;                                                    // no need add row
  } else if (!is_fetch_with_ties_ && heap_.count() == get_topn_cnt()) {  // adjust heap
    if (heap_.count() == 0) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(adjust_topn_heap(row))) {
      LOG_WARN("failed to adjust topn heap", K(ret));
    }
  } else {  // push back array
    static const int64_t HEADER_SIZE = sizeof(RowWrapper);
    RowWrapper* new_row = NULL;
    char* buf = NULL;
    // optimize for hit-rate: enlarge first Limit-Count row's space,
    // so following rows are more likely to fit in.
    int64_t buffer_len = HEADER_SIZE + 2 * row.get_deep_copy_size();
    if (OB_ISNULL(buf = reinterpret_cast<char*>(cur_alloc_->alloc(buffer_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc buf failed", K(ret));
    } else if (OB_ISNULL(new_row = new (buf) RowWrapper())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to new row", K(ret));
    } else {
      new_row->buf_len_ = buffer_len;
      int64_t pos = HEADER_SIZE;
      if (OB_FAIL(new_row->row_.deep_copy(row, buf, buffer_len, pos))) {
        LOG_WARN("failed to deep copy row", K(ret), K(buffer_len));
      } else if (OB_FAIL(heap_.push(new_row))) {
        LOG_WARN("failed to push back row", K(ret), K(buffer_len));
      } else {
        LOG_DEBUG("in memory topn sort check add row", KPC(new_row));
        last_row_ = &new_row->row_;
      }
    }
  }
  return ret;
}

int ObInMemoryTopnSort::adjust_topn_heap(const ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(heap_.top())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error.top of the heap is NULL", K(ret), K(topn_sort_array_pos_), K(heap_.count()));
  } else if (!heap_.empty()) {
    if (cmp_(row, heap_.top()->row_)) {
      static const int64_t HEADER_SIZE = sizeof(RowWrapper);
      RowWrapper* new_row = NULL;
      RowWrapper* dt_row = heap_.top();
      char* buf = NULL;
      int64_t size = row.get_deep_copy_size();
      int64_t buffer_len = 0;
      // check to see whether this old row's space is adequate for new one
      if (dt_row->buf_len_ >= size + HEADER_SIZE) {
        // Oh yeah
        buf = reinterpret_cast<char*>(dt_row);
        new_row = dt_row;
        buffer_len = dt_row->buf_len_;
      } else {
        buffer_len = size * 2 + HEADER_SIZE;
        if (OB_ISNULL(buf = reinterpret_cast<char*>(cur_alloc_->alloc(buffer_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc buf failed", K(ret));
        } else if (OB_ISNULL(new_row = new (buf) RowWrapper())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("failed to new row", K(ret));
        } else {
          new_row->buf_len_ = buffer_len;
        }
      }
      if (OB_SUCC(ret)) {
        int64_t pos = HEADER_SIZE;
        if (OB_FAIL(new_row->row_.deep_copy(row, buf, buffer_len, pos))) {
          LOG_WARN("failed to deep copy row", K(ret), K(buffer_len));
        } else if (OB_FAIL(heap_.replace_top(new_row))) {
          LOG_WARN("failed to replace top", K(ret));
        } else {
          LOG_DEBUG("in memory topn sort check replace row", KPC(new_row));
          last_row_ = &new_row->row_;
        }
      }
    }
  }
  return ret;
}

int ObInMemoryTopnSort::sort_rows()
{
  int ret = OB_SUCCESS;
  if (0 == heap_.count()) {
    // do nothing
  } else if (!is_fetch_with_ties_ && get_topn_cnt() < heap_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("topn is less than array_count", K(ret), K(get_topn_cnt()), K(heap_.count()));
  } else {
    LOG_DEBUG("in memory topn sort check topn heap", K_(heap));
    RowWrapper** first_row = &heap_.top();
    std::sort(first_row, first_row + heap_.count(), cmp_);
  }
  return ret;
}

int ObInMemoryTopnSort::get_next_row(ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (topn_sort_array_pos_ < 0) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("sort array out of range", K(ret), K_(topn_sort_array_pos));
  } else if (topn_sort_array_pos_ >= heap_.count()) {
    ret = OB_ITER_END;
    SQL_ENG_LOG(DEBUG, "end of the in-memory run");
    if (topn_sort_array_pos_ > 0) {
      // Reset status when iterating end, because we will add rows and sort again after dumped to disk.
      topn_sort_array_pos_ = 0;
      heap_.reset();
      cur_alloc_->reset();
      last_row_ = NULL;
    }
  } else if (OB_ISNULL(heap_.at(topn_sort_array_pos_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. row is null", K(ret), K(topn_sort_array_pos_), K(heap_.count()));
  } else {
    const ObNewRow& tmp_row = heap_.at(topn_sort_array_pos_)->row_;
    if (OB_FAIL(attach_row(tmp_row, row))) {
      LOG_WARN("fail to get row", K(ret));
    } else {
      ++topn_sort_array_pos_;
    }
  }
  return ret;
}

int64_t ObInMemoryTopnSort::get_row_count() const
{
  return heap_.count();
}

int64_t ObInMemoryTopnSort::get_used_mem_size() const
{
  return heap_.count() * sizeof(void*) + cur_alloc_->used();
}

int ObInMemoryTopnSort::get_next_compact_row(ObString& compact_row)
{
  UNUSED(compact_row);
  int ret = OB_SUCCESS;
  return ret;
}

int ObInMemoryTopnSort::set_sort_columns(const common::ObIArray<ObSortColumn>& sort_columns, const int64_t prefix_pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBaseSort::set_sort_columns(sort_columns, prefix_pos))) {
    LOG_WARN("set sort column failed", K(ret));
  } else {
    cmp_.sort_columns_ = &sort_columns;
  }
  return ret;
}
