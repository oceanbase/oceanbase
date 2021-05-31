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

#include "sql/engine/sort/ob_local_merge_sort.h"

#include "lib/utility/utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_serialization_helper.h"
#include "lib/oblog/ob_log_module.h"
#include "common/object/ob_obj_compare.h"
#include "sql/engine/ob_exec_context.h"

#include "lib/utility/ob_tracepoint.h"
#include "lib/time/ob_time_utility.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

#define MAX_INPUT_NUMBER 10000

bool ObLMSRowCompare::operator()(int64_t row_idx1, int64_t row_idx2)
{
  int& ret = ret_;
  int cmp = 0;
  bool cmp_ret = false;
  if (OB_FAIL(ret)) {
    // do nothing if we already have an error,
    // so we can finish the sort process ASAP.
  } else if (OB_ISNULL(columns_) || OB_ISNULL(rows_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("columns or rows is NULL", K(ret), K_(columns), K_(rows));
  } else if (OB_UNLIKELY(!(0 <= row_idx1 && row_idx1 < rows_->count() && 0 <= row_idx2 && row_idx2 < rows_->count()))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("row idx out of range", K(ret), K(row_idx1), K(row_idx2), K(rows_->count()));
  } else {
    const ObNewRow* row1 = rows_->at(row_idx1);
    const ObNewRow* row2 = rows_->at(row_idx2);
    if (OB_UNLIKELY(OB_ISNULL(row1) || OB_ISNULL(row2))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row should not be null", K(row_idx1), K(row_idx2), "total", rows_->count(), K(ret));
    }
    ObCompareCtx cmp_ctx;
    cmp_ctx.cmp_type_ = ObMaxType;
    cmp_ctx.cmp_cs_type_ = CS_TYPE_INVALID;
    cmp_ctx.is_null_safe_ = true;
    cmp_ctx.tz_off_ = INVALID_TZ_OFF;
    // it must use get_cell
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < columns_->count(); i++) {
      int64_t col_idx = columns_->at(i).index_;
      if (OB_UNLIKELY(!(0 <= col_idx && col_idx < row1->count_ && col_idx < row2->count_))) {
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("row idx out of range", K(ret), K(col_idx), K(row1->count_), K(row2->count_));
      } else {
        cmp_ctx.cmp_cs_type_ = columns_->at(i).cs_type_;
        cmp_ctx.null_pos_ = columns_->at(i).get_cmp_null_pos();
        cmp = row1->get_cell(col_idx).compare(row2->get_cell(col_idx), cmp_ctx);
        if (cmp < 0) {
          cmp_ret = !columns_->at(i).is_ascending();
        } else if (cmp > 0) {
          cmp_ret = columns_->at(i).is_ascending();
        }
      }
    }
  }
  return cmp_ret;
}

ObLocalMergeSort::ObLocalMergeSort()
    : is_finish_(false),
      is_heap_get_row_(true),
      cur_input_(INT64_MAX),
      last_group_end_pos_(0),
      is_last_row_same_group_(true),
      last_row_(NULL),
      input_readers_(),
      row_heap_(),
      cmp_row_arr_(),
      cmp_fun_()
{}

ObLocalMergeSort::~ObLocalMergeSort()
{
  reset();
}

void ObLocalMergeSort::reset()
{
  cleanup();
  ObBaseSort::reset();
  is_finish_ = false;
  is_heap_get_row_ = true;
  last_group_end_pos_ = 0;
  is_last_row_same_group_ = true;
  cur_input_ = INT64_MAX;
  last_row_ = NULL;
  input_readers_.reset();
  cmp_row_arr_.reset();
}

void ObLocalMergeSort::reuse()
{
  cleanup();
  ObBaseSort::reuse();
  is_finish_ = false;
  is_heap_get_row_ = true;
  last_group_end_pos_ = 0;
  is_last_row_same_group_ = true;
  cur_input_ = INT64_MAX;
  last_row_ = NULL;
  input_readers_.reset();
  cmp_row_arr_.reset();
}

int ObLocalMergeSort::set_sort_columns(const common::ObIArray<ObSortColumn>& sort_columns, const int64_t prefix_pos)
{
  int ret = OB_SUCCESS;
  if (0 != prefix_pos) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prefix pos is 0 in local merge sort", K(prefix_pos), K(ret));
  } else if (OB_FAIL(ObBaseSort::set_sort_columns(sort_columns, prefix_pos))) {
    LOG_WARN("set sort column failed", K(ret));
  } else if (OB_FAIL(cmp_row_arr_.prepare_allocate(2))) {
    LOG_WARN("fail to init array", K(ret));
  } else {
    cmp_fun_.init(sort_columns, cmp_row_arr_);
  }
  return ret;
}

int64_t ObLocalMergeSort::get_used_mem_size() const
{
  return cmp_row_arr_.count() * sizeof(void*) + cur_alloc_->used() + next_block_alloc_->used();
}

int ObLocalMergeSort::cleanup()
{
  int ret = OB_SUCCESS;
  int release_merge_sort_ret = OB_SUCCESS;
  while (OB_SUCC(ret) && 0 < input_readers_.count()) {
    InputReader* reader = NULL;
    release_merge_sort_ret = input_readers_.pop_back(reader);
    if (OB_SUCCESS != release_merge_sort_ret) {
      LOG_WARN("pop back row store failed", K(release_merge_sort_ret));
    } else {
      reader->~InputReader();
      cur_alloc_->free(reader);
    }
  }
  return ret;
}

// new group, the range is [start_pos, end_pos)
int ObLocalMergeSort::alloc_reader(int64_t& start_pos, int64_t end_pos)
{
  int ret = OB_SUCCESS;
  if (input_readers_.count() > MAX_INPUT_NUMBER) {
    // only log
    LOG_WARN("too many local order inputs", K(MAX_INPUT_NUMBER), K(ret));
  }
  void* buf = cur_alloc_->alloc(sizeof(InputReader));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create ra row store fail", K(ret));
  } else {
    InputReader* reader = new (buf) InputReader(start_pos, end_pos);
    if (OB_FAIL(input_readers_.push_back(reader))) {
      LOG_WARN("fail push back MergeSortInput", K(ret));
    } else {
      LOG_DEBUG("new group:", K(start_pos), K(end_pos));
      start_pos = end_pos;
    }
  }
  return ret;
}

int ObLocalMergeSort::sort_rows()
{
  int ret = OB_SUCCESS;
  set_finish(true);
  // when child is iterator end, process last group
  if (OB_FAIL(alloc_reader(last_group_end_pos_, sort_array_.count()))) {
    LOG_WARN("fail to alloc reader for last group", K(ret));
  } else if (OB_FAIL(row_heap_.init(input_readers_.count(), *get_sort_columns()))) {
    LOG_WARN("fail to init row heap", K(ret));
  }
  return ret;
}

int ObLocalMergeSort::inner_add_row(const common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  ObNewRow* new_row = NULL;
  if (OB_FAIL(ObPhyOperator::deep_copy_row(row, new_row, *cur_alloc_))) {
    LOG_WARN("deep copy a new row failed", K(ret));
  } else if (OB_ISNULL(new_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new row is null", K(ret));
  } else if (OB_FAIL(add_typed_row(*new_row, *cur_alloc_))) {
    LOG_WARN("failed to add typed row", K(ret));
  }
  return ret;
}

int ObLocalMergeSort::add_row(const common::ObNewRow& row, bool& need_sort)
{
  int ret = OB_SUCCESS;
  UNUSED(need_sort);
  const ObNewRow* last_row = NULL;
  int64_t cur_pos = sort_array_.count();
  if (cur_pos == 0) {
    if (OB_NOT_NULL(last_row_)) {
      cmp_row_arr_.at(0) = last_row_;
      cmp_row_arr_.at(1) = &row;
      is_last_row_same_group_ = cmp_fun_(0, 1);
      last_row_ = nullptr;
      next_block_alloc_->reset();
    }
    // first row
    if (OB_FAIL(inner_add_row(row))) {
      LOG_WARN("fail to add row to array", K(ret));
    }
  } else if (OB_ISNULL(last_row = get_last_row())) {
    LOG_WARN("fail to get last row", K(ret));
  } else {
    cmp_row_arr_.at(0) = last_row;
    cmp_row_arr_.at(1) = &row;
    bool is_new_group = cmp_fun_(0, 1);
    if (OB_SUCC(cmp_fun_.get_ret()) && is_new_group && OB_FAIL(alloc_reader(last_group_end_pos_, cur_pos))) {
      LOG_WARN("fail to alloc new group reader", K(last_group_end_pos_), K(cur_pos), K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(inner_add_row(row))) {
      LOG_WARN("fail to add row to sort array", K(ret));
    } else { /* nothing to do*/
    }
  }
  return ret;
}

int ObLocalMergeSort::get_next_row(common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (is_heap_get_row_) {
    if (OB_FAIL(heap_get_next_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row in merge sort", K(ret));
      }
    }
  } else {
    if (INT64_MAX == cur_input_ || 0 > cur_input_ || cur_input_ >= input_readers_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid nth input", K(ret));
    } else {
      InputReader* reader = input_readers_.at(cur_input_);
      const ObNewRow* in_row = nullptr;
      if (OB_FAIL(reader->get_row(in_row, sort_array_))) {
        if (OB_ITER_END == ret) {
          LOG_WARN("finish to get nth input", K(cur_input_), K(ret));
        }
      } else if (OB_ISNULL(in_row) || OB_FAIL(attach_row(*in_row, row))) {
        LOG_WARN("fail to get row", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalMergeSort::InputReader::get_row(
    const common::ObNewRow*& row, common::ObArray<const ObBaseSort::StrongTypeRow*>& sort_array)
{
  int ret = OB_SUCCESS;
  if (cur_pos_ < 0 || cur_pos_ > end_pos_) {
    // last row should be max_pos
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid row pos", K(cur_pos_), K(ret));
  } else {
    if (cur_pos_ >= end_pos_) {
      // fetch all data already
      ret = OB_ITER_END;
      LOG_TRACE("finish to fetch all data from one input", K(cur_pos_), K(start_pos_), K(end_pos_), K(ret));
    } else if (OB_ISNULL(sort_array.at(cur_pos_)) || OB_ISNULL(row = sort_array.at(cur_pos_)->row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail get row", K(cur_pos_), K(ret));
    } else {
      cur_pos_++;
    }
  }
  return ret;
}

int ObLocalMergeSort::heap_get_next_row(common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && row_heap_.capacity() > row_heap_.count()) {
    const ObNewRow* in_row = NULL;
    InputReader* reader = input_readers_.at(row_heap_.writable_channel_idx());
    if (OB_ISNULL(reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to input reader", K(ret));
    } else if (OB_FAIL(reader->get_row(in_row, sort_array_))) {
      if (OB_ITER_END == ret) {
        row_heap_.shrink();
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(in_row)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(row_heap_.push(in_row))) {
      LOG_WARN("fail push row to heap", K(ret));
    } else { /* nothing */
    }
  }

  if (OB_SUCC(ret)) {
    if (0 == row_heap_.capacity()) {
      ret = OB_ITER_END;
      cleanup();
      sort_array_.reset();
      cur_alloc_->reset();
    } else if (row_heap_.capacity() == row_heap_.count()) {
      const ObNewRow* in_row = NULL;
      if (OB_FAIL(row_heap_.pop(in_row))) {
        LOG_WARN("fail pop row from heap", K(ret));
      } else if (OB_FAIL(attach_row(*in_row, row))) {
        LOG_WARN("fail to get row", K(ret));
      } else {
        LOG_DEBUG("get row", K(ret), K(*in_row));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid row heap state", K(ret));
    }
  }
  return ret;
}

// save last row and compare whether it's same group for next dump
int ObLocalMergeSort::save_last_row()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(last_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect last row is null", K(ret));
  } else if (sort_array_.count() > 0) {
    if (OB_ISNULL(get_last_row())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last row is null", K(ret), K(get_last_row()));
    } else if (OB_FAIL(ObPhyOperator::deep_copy_row(*get_last_row(), last_row_, *next_block_alloc_))) {
      LOG_WARN("failed to copy last row", K(ret));
    }
  }
  return ret;
}

int ObLocalMergeSort::final_dump(ObIMergeSort& merge_sort)
{
  return inner_dump(merge_sort, true);
}

int ObLocalMergeSort::dump(ObIMergeSort& merge_sort)
{
  return inner_dump(merge_sort, false);
}

int ObLocalMergeSort::inner_dump(ObIMergeSort& merge_sort, bool dump_last)
{
  int ret = OB_SUCCESS;
  if (!is_last_row_same_group_ && OB_FAIL(merge_sort.build_cur_fragment())) {
    LOG_WARN("fail to build fragment", K(ret));
  } else {
    set_heap_get_row(false);
    int64_t total_input = input_readers_.count();
    for (int64_t nth_input = 0; OB_SUCC(ret) && nth_input < total_input; ++nth_input) {
      set_cur_input(nth_input);
      bool last_group_local_order_data = (nth_input == total_input - 1);
      if (last_group_local_order_data) {
        // last group local order data, save last row and don't build fragment
        if (OB_FAIL(save_last_row())) {
          LOG_WARN("fail to save last row data", K(ret));
        }
      }
      // If first group local data is same as last group, then add same fragment
      if (OB_SUCC(ret) && OB_FAIL(merge_sort.dump_base_run(*this, dump_last ? true : !last_group_local_order_data))) {
        LOG_WARN("fail to dump data", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      cleanup();
      is_heap_get_row_ = true;
      last_group_end_pos_ = 0;
      cur_input_ = INT64_MAX;
      is_last_row_same_group_ = true;
      input_readers_.reset();
      sort_array_.reset();
      cur_alloc_->reset();
    }
  }
  return ret;
}
