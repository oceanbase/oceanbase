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

#include "sql/engine/sort/ob_specific_columns_sort.h"
#include "common/row/ob_row_util.h"
#include "common/object/ob_obj_compare.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

// init static member value

ObSpecificColumnsSort::ObSpecificColumnsSort()
    : row_alloc_(ObModIds::OB_SQL_SORT_ROW, OB_MALLOC_NORMAL_BLOCK_SIZE, OB_SERVER_TENANT_ID, ObCtxIds::WORK_AREA),
      row_count_(0),
      prefix_keys_pos_(0),
      row_array_pos_(0),
      next_block_row_(NULL),
      sort_array_(),
      sort_columns_(NULL),
      obobj_buffer_(NULL),
      err_(OB_SUCCESS)
{}

ObSpecificColumnsSort::ObSpecificColumnsSort(
    const char* label, uint64_t malloc_block_size, uint64_t tenant_id, ObCtxIds::ObCtxIdEnum ctx_id)
    : row_alloc_(label, malloc_block_size, tenant_id, ctx_id),
      row_count_(0),
      prefix_keys_pos_(0),
      row_array_pos_(0),
      next_block_row_(NULL),
      sort_array_(),
      sort_columns_(NULL),
      obobj_buffer_(NULL),
      err_(OB_SUCCESS)
{}

void ObSpecificColumnsSort::reset()
{
  row_count_ = 0;
  prefix_keys_pos_ = 0;
  row_array_pos_ = 0;
  next_block_row_ = NULL;
  sort_columns_ = NULL;
  row_alloc_.reset();
  sort_array_.reset();
  obobj_buffer_ = NULL;
}

void ObSpecificColumnsSort::reuse()
{
  reset();
}

// just for roll up
void ObSpecificColumnsSort::rescan()
{
  row_array_pos_ = 0;
  next_block_row_ = NULL;
}

int ObSpecificColumnsSort::set_sort_columns(const ObIArray<ObSortColumn>& sort_columns, const int64_t prefix_pos)
{
  int ret = OB_SUCCESS;
  sort_columns_ = &sort_columns;
  ret = ObBaseSort::set_sort_columns(sort_columns, prefix_pos);
  if (OB_SUCC(ret)) {
    prefix_keys_pos_ = prefix_pos;
  } else {
    // do nothing
  }
  return ret;
}

int ObSpecificColumnsSort::deep_copy_new_row(const ObNewRow& row, ObNewRow*& new_row, ObArenaAllocator& alloc)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t pos = sizeof(ObNewRow);
  int64_t buf_len = row.get_deep_copy_size() + sizeof(ObNewRow);
  if (OB_ISNULL(buf = static_cast<char*>(alloc.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc new row failed", K(ret), K(buf_len));
  } else if (OB_ISNULL(new_row = new (buf) ObNewRow())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_row is null", K(ret), K(buf_len));
  } else if (OB_FAIL(new_row->deep_copy(row, buf, buf_len, pos))) {
    LOG_WARN("deep copy row failed", K(ret), K(buf_len), K(pos));
  } else {
    // do nothing
  }
  return ret;
}

int ObSpecificColumnsSort::get_sort_result_array(common::ObArray<const common::ObNewRow*>& sort_result)
{
  sort_result = sort_array_;
  return OB_SUCCESS;
};

int ObSpecificColumnsSort::add_row(const common::ObNewRow& row, bool& need_sort)
{
  int ret = OB_SUCCESS;
  bool is_cur_block = false;
  need_sort = false;
  const ObNewRow* last_row = NULL;
  if (sort_array_.count() > 0) {
    last_row = sort_array_.at(sort_array_.count() - 1);
  }
  if (OB_FAIL(check_block_row(row, last_row, is_cur_block))) {
    LOG_WARN("check if row is belong to cur block failed", K(ret));
  } else {
    ObNewRow* new_row = NULL;
    // if (need_deep_copy) {
    if (OB_FAIL(deep_copy_new_row(row, new_row, row_alloc_))) {
      LOG_WARN("deep copy a new row failed", K(ret));
    } else if (OB_ISNULL(new_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new row is null", K(ret));
    } else if (is_cur_block) {
      if (OB_FAIL(sort_array_.push_back(new_row))) {
        LOG_WARN("push back new row failed", K(ret));
      }
    } else {
      need_sort = true;
      next_block_row_ = new_row;  // new row is next sort block row
    }
  }
  return ret;
}

int ObSpecificColumnsSort::add_row_without_copy(common::ObNewRow* row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new row is null", K(ret));
  } else if (OB_FAIL(sort_array_.push_back(row))) {
    LOG_WARN("push back new row failed", K(ret));
  }
  return ret;
}

// check row and last_row are belong to same sort block
// such as prefix column keys is (a,b)
// (1,2,3) and (1,2,4) are same block
int ObSpecificColumnsSort::check_block_row(const ObNewRow& row, const ObNewRow* last_row, bool& is_cur_block)
{
  int ret = OB_SUCCESS;
  is_cur_block = true;
  if (!has_prefix_pos() || NULL == last_row) {
    // do nothing, need add row
  } else {
    int cmp = 0;
    const ObIArray<ObSortColumn>* sort_column = get_sort_columns();
    if (OB_ISNULL(sort_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sort column is null", K(ret));
    }
    for (int64_t i = 0; 0 == cmp && OB_SUCC(ret) && i < prefix_keys_pos_; i++) {
      int64_t idx = sort_column->at(i).index_;
      ObCompareCtx cmp_ctx(
          ObMaxType, sort_column->at(i).cs_type_, true, INVALID_TZ_OFF, sort_column->at(i).get_cmp_null_pos());
      cmp = row.get_cell(idx).compare(last_row->get_cell(idx), cmp_ctx);
      if ((cmp < 0 && sort_column->at(i).is_ascending()) ||
          (cmp > 0 && !sort_column->at(i).is_ascending())) {  // check prefix_sort_columns
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("prefix sort ordering is invalid", K(ret), K(row), K(*last_row), K(sort_column->at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == cmp) {  // row is equal last_row with prefix sort keys
        ++row_count_;
      } else {
        is_cur_block = false;
      }
    }
  }
  return ret;
}

int ObSpecificColumnsSort::specific_columns_cmp(const void* p1, const void* p2)
{
  bool bret = false;
  ObSpecificColumnsSort** scs1 = (ObSpecificColumnsSort**)p1;
  ObSpecificColumnsSort** scs2 = (ObSpecificColumnsSort**)p2;
  if (OB_ISNULL(scs1) || OB_ISNULL(scs2)) {
    LOG_WARN("scs1 is null or scs2 is null", K(bret));
  } else if (OB_ISNULL(*scs1) || OB_ISNULL(*scs2)) {
    LOG_WARN("*scs1 is null or *scs2 is null", K(bret));
  } else if (OB_UNLIKELY(OB_SUCCESS != (*scs1)->err_) || OB_UNLIKELY(OB_SUCCESS != (*scs2)->err_)) {
    // do nothing if we already have an error,
    // so we can finish the sort process ASAP.
  } else if (((*scs1) != (*scs1))) {
    (*scs1)->err_ = OB_ERR_UNEXPECTED;
    (*scs2)->err_ = OB_ERR_UNEXPECTED;
  } else {
    int cmp = 0;
    int64_t prefix_sort_columns_count = (*scs1)->sort_columns_->count() - (*scs1)->prefix_keys_pos_;
    int64_t prefix_keys_pos = (*scs1)->prefix_keys_pos_;
    ObSpecificColumnsSort* scs = *scs1;
    ObObj* r1 = (ObObj*)(scs1 + 1);
    ObObj* r2 = (ObObj*)(scs2 + 1);
    ObCompareCtx cmp_ctx;
    cmp_ctx.cmp_type_ = ObMaxType;
    cmp_ctx.cmp_cs_type_ = CS_TYPE_INVALID;
    cmp_ctx.is_null_safe_ = true;
    cmp_ctx.tz_off_ = INVALID_TZ_OFF;
    for (int64_t i = 0; OB_SUCCESS == (scs)->err_ && 0 == cmp && i < prefix_sort_columns_count; ++i) {
      // int64_t idx = ObSpecificColumnsSort::sort_columns_->at(i).index_;
      cmp_ctx.cmp_cs_type_ = (scs)->sort_columns_->at(prefix_keys_pos + i).cs_type_;
      cmp_ctx.null_pos_ = (scs)->sort_columns_->at(prefix_keys_pos + i).get_cmp_null_pos();
      cmp = r1[i].compare(r2[i], cmp_ctx);
      if (cmp < 0) {
        bret = (scs)->sort_columns_->at(prefix_keys_pos + i).is_ascending();
      } else if (cmp > 0) {
        bret = !((scs)->sort_columns_->at(prefix_keys_pos + i).is_ascending());
      } else {
        // do nothing
      }
    }  // end for
  }
  return bret ? -1 : 1;
}

int ObSpecificColumnsSort::init_specific_columns()
{
  int ret = OB_SUCCESS;
  ObObj* rows_buf = NULL;
  ObSpecificColumnsSort** scs = NULL;
  if (sort_array_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the sort_array_ is empty", K(ret));
  } else if (OB_ISNULL(obobj_buffer_ = static_cast<char*>(
                           row_alloc_.alloc((sizeof(ObObj) * (sort_columns_->count() - prefix_keys_pos_) +
                                                sizeof(ObNewRow*) + sizeof(ObSpecificColumnsSort*)) *
                                            sort_array_.count())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for obobj buffer", K(ret));
  } else if (OB_ISNULL(scs = reinterpret_cast<ObSpecificColumnsSort**>(obobj_buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the first ptr in obobj buffer is null", K(ret));
  } else {
    // mem view [ this obj obj obj obnewrow*]
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_array_.count(); i++) {
      // only contain the columns that should be sorted.
      int64_t buf_offset = 0;
      *scs = this;
      scs = scs + 1;
      rows_buf = reinterpret_cast<ObObj*>(scs);
      ObNewRow* row = const_cast<ObNewRow*>(sort_array_.at(i));
      if (OB_ISNULL(rows_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the pointer is null", K(ret));
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("some of the ObNewRow* in sort_array_  is null", K(ret), K(i));
      } else {
        int64_t idx = 0;
        for (int64_t j = 0; OB_SUCC(ret) && j < (sort_columns_->count() - prefix_keys_pos_); j++) {
          idx = sort_columns_->at(prefix_keys_pos_ + j).index_;
          MEMCPY(rows_buf + buf_offset, &(row->get_cell(idx)), sizeof(ObObj));
          ++buf_offset;
        }
        if (OB_SUCC(ret)) {
          rows_buf = rows_buf + (sort_columns_->count() - prefix_keys_pos_);
          // add raw obnewrow ptr to the space's tail
          ObNewRow** temp_ob_new_row = reinterpret_cast<ObNewRow**>(rows_buf);
          if (OB_ISNULL(temp_ob_new_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("the pointer is null", K(ret));
          } else {
            *temp_ob_new_row = row;
            if (OB_ISNULL(scs = reinterpret_cast<ObSpecificColumnsSort**>(temp_ob_new_row + 1))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("the pointer is null", K(ret));
            }
          }
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObSpecificColumnsSort::sort_rows()
{
  // LOG_INFO("Specific Columns Working");
  int ret = OB_SUCCESS;
  void* first_row = NULL;
  const ObIArray<ObSortColumn>* sort_column = NULL;
  if (OB_ISNULL(sort_column = get_sort_columns())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort column is null", K(ret));
  } else if (prefix_keys_pos_ > sort_column->count() || 0 == sort_column->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err, prefix_keys_pos should be smaller than column count",
        K(prefix_keys_pos_),
        K(sort_column->count()));
  } else if (0 == sort_array_.count()) {
    // do nothing
  } else if (OB_FAIL(init_specific_columns())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("err happen in specific column sort init, failed to do sort", K(ret));
  } else if (OB_ISNULL(first_row = static_cast<void*>(obobj_buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ptr", K(ret));
  } else {
    int64_t fixed_offset = (sort_columns_->count() - prefix_keys_pos_) * sizeof(ObObj) + sizeof(ObNewRow**) +
                           sizeof(ObSpecificColumnsSort**);
    std::qsort(first_row, sort_array_.count(), fixed_offset, specific_columns_cmp);
    if (OB_SUCCESS != err_) {
      ret = err_;
      LOG_WARN("failed to do qsort", K(ret));
    } else {
      char* cur_buf = obobj_buffer_;
      cur_buf = cur_buf + (sort_columns_->count() - prefix_keys_pos_) * sizeof(ObObj) + sizeof(ObSpecificColumnsSort**);
      for (int64_t i = 0; i < sort_array_.count(); i++) {
        ObNewRow** tmp = (reinterpret_cast<ObNewRow**>(cur_buf));
        if (OB_ISNULL(tmp)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL ptr", K(ret));
          break;
        } else {
          sort_array_.at(i) = *tmp;
          cur_buf = cur_buf + fixed_offset;
        }
      }
    }
  }
  if (OB_ISNULL(obobj_buffer_)) {
  } else {
    row_alloc_.free(obobj_buffer_);
    obobj_buffer_ = NULL;
  }
  return ret;
}

int ObSpecificColumnsSort::get_next_row(common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  const ObNewRow* tmp_row = NULL;
  if (row_array_pos_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row array pos is invalid", K(row_array_pos_), K(sort_array_.count()));
  } else if (row_array_pos_ >= sort_array_.count()) {
    if (NULL != next_block_row_) {
      row_array_pos_ = 0;
      sort_array_.reset();
      sort_array_.push_back(next_block_row_);
      next_block_row_ = NULL;
    }
    ret = OB_ITER_END;
  } else if (OB_ISNULL(tmp_row = sort_array_.at(row_array_pos_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp row is null", K(ret), K(row_array_pos_));
  } else if (tmp_row->get_count() > row.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array row count is invalid", K(ret), K(tmp_row->count_), K(row.count_));
  } else {
    int64_t idx = 0;
    while (idx < tmp_row->get_count()) {
      row.cells_[idx] = tmp_row->get_cell(idx);
      ++idx;
    }
    ++row_array_pos_;
  }
  return ret;
}
