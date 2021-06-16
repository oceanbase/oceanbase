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

#include "sql/engine/sort/ob_base_sort.h"
#include "common/object/ob_obj_compare.h"
#include "common/row/ob_row_util.h"
#include "share/ob_cluster_version.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObSortableTrait::add_sort_column(
    const int64_t index, ObCollationType cs_type, bool is_ascending, ObObjType obj_type, ObOrderDirection order_type)
{
  int ret = OB_SUCCESS;
  ObSortColumn sort_column;
  sort_column.index_ = index;
  sort_column.cs_type_ = cs_type;
  sort_column.extra_info_ = 0;
  if (is_ascending) {
    sort_column.extra_info_ |= ObSortColumnExtra::SORT_COL_ASC_BIT;
  } else {
    sort_column.extra_info_ &= ObSortColumnExtra::SORT_COL_ASC_MASK;
  }
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2240) {
    sort_column.extra_info_ |= ObSortColumnExtra::SORT_COL_EXTRA_BIT;
  } else {
    sort_column.extra_info_ &= ObSortColumnExtra::SORT_COL_EXTRA_MASK;
  }
  sort_column.obj_type_ = obj_type;
  sort_column.order_type_ = order_type;
  if (OB_FAIL(sort_columns_.push_back(sort_column))) {
    LOG_WARN("failed to push back to array", K(ret));
  }
  return ret;
}

int ObSortableTrait::init_sort_columns(int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(count), K(ret));
  } else if (OB_FAIL(sort_columns_.init(count))) {
    LOG_WARN("fail to init array", K(count), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObBaseSort::StrongTypeRow::init(common::ObIAllocator& alloc, int64_t obj_cnt)
{
  int ret = OB_SUCCESS;
  void* buf = nullptr;
  int64_t buf_size = sizeof(ObObj) * obj_cnt;
  if (buf_size <= 0) {  // primary key or index, there will be order itself, buf_siz == 0
    LOG_DEBUG("prefix sort column length is 0");
  } else if (OB_ISNULL(buf = alloc.alloc(buf_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(buf_size));
  } else {
    objs_ = static_cast<ObObj*>(buf);
  }
  return ret;
}

ObBaseSort::ObBaseSort()
    : topn_cnt_(INT64_MAX),
      row_alloc0_(ObModIds::OB_SQL_SORT_ROW, OB_MALLOC_NORMAL_BLOCK_SIZE, OB_SERVER_TENANT_ID, ObCtxIds::WORK_AREA),
      row_alloc1_(ObModIds::OB_SQL_SORT_ROW, OB_MALLOC_NORMAL_BLOCK_SIZE, OB_SERVER_TENANT_ID, ObCtxIds::WORK_AREA),
      cur_alloc_(&row_alloc0_),
      next_block_alloc_(&row_alloc1_),
      sort_array_(),
      row_count_(0),
      prefix_keys_pos_(0),
      row_array_pos_(0),
      next_block_row_(NULL),
      sort_columns_(NULL),
      prefix_sort_columns_()
{}

void ObBaseSort::reset()
{

  topn_cnt_ = INT64_MAX;
  row_count_ = 0;
  prefix_keys_pos_ = 0;
  row_array_pos_ = 0;
  next_block_row_ = NULL;
  cur_alloc_->reset();
  next_block_alloc_->reset();
  sort_array_.reset();
  sort_columns_ = NULL;
  prefix_sort_columns_.reset();
}

void ObBaseSort::reuse()
{
  reset();
}

// just for roll up
void ObBaseSort::rescan()
{
  row_array_pos_ = 0;
  next_block_row_ = NULL;
}

int ObBaseSort::set_sort_columns(const ObIArray<ObSortColumn>& sort_columns, const int64_t prefix_pos)
{
  int ret = OB_SUCCESS;
  sort_columns_ = &sort_columns;
  prefix_keys_pos_ = prefix_pos;
  for (int64_t i = prefix_pos; OB_SUCC(ret) && i < sort_columns.count(); ++i) {
    if (OB_FAIL(prefix_sort_columns_.push_back(sort_columns.at(i)))) {
      LOG_WARN("push back sort item failed", K(ret));
    }
  }
  LOG_DEBUG("set sort columns", K(sort_columns), K(prefix_sort_columns_), K(prefix_pos));
  return ret;
}

struct ObBaseSort::TypedRowComparer {
  explicit TypedRowComparer(int* err)
      : inited_(false), sort_columns_(NULL), null_poses_(NULL), sort_funcs_(NULL), cnt_(0), err_(err)
  {}

  int init(ObIAllocator& alloc, const ObIArray<ObSortColumn>& sort_columns)
  {
    int ret = OB_SUCCESS;

    obj_cmp_func_nullsafe tmp_cmp_func = NULL;
    void* func_buf = NULL;
    void* col_buf = NULL;
    void* null_pos_buf = NULL;
    int64_t func_buf_size = sizeof(obj_cmp_func_nullsafe) * sort_columns.count();
    int64_t col_buf_size = sizeof(ObSortColumn) * sort_columns.count();
    int64_t null_pos_buf_size = sizeof(ObCmpNullPos) * sort_columns.count();
    if (func_buf_size <= 0 || col_buf_size <= 0) {
      // do nothing
    } else if (OB_ISNULL(func_buf = alloc.alloc(func_buf_size)) || OB_ISNULL(col_buf = alloc.alloc(col_buf_size)) ||
               OB_ISNULL(null_pos_buf = alloc.alloc(null_pos_buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(col_buf_size), K(func_buf_size));
    } else {
      sort_funcs_ = static_cast<obj_cmp_func_nullsafe*>(func_buf);
      sort_columns_ = static_cast<ObSortColumn*>(col_buf);
      null_poses_ = static_cast<ObCmpNullPos*>(null_pos_buf);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_columns.count(); i++) {
      if (false ==
          ObObjCmpFuncs::can_cmp_without_cast(
              sort_columns.at(i).get_obj_type(), sort_columns.at(i).get_obj_type(), common::CO_CMP, tmp_cmp_func)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null cmp func", K(ret), K(tmp_cmp_func), K(sort_columns.at(i).get_obj_type()));
      } else if (MAX_DIR == sort_columns.at(i).get_order_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid direction", K(ret), K(sort_columns.at(i).get_order_type()));
      } else {
        sort_funcs_[i] = tmp_cmp_func;
        sort_columns_[i] = sort_columns.at(i);
        // null first: null is smaller than any value
        // null last: null is greater than any value
        null_poses_[i] = sort_columns.at(i).get_cmp_null_pos();
        tmp_cmp_func = NULL;
      }
    }
    if (OB_SUCC(ret)) {
      cnt_ = sort_columns.count();
      inited_ = true;
    }
    return ret;
  }

  bool operator()(const StrongTypeRow* r1, const StrongTypeRow* r2)
  {
    bool bret = false;
    if (OB_UNLIKELY(OB_SUCCESS != *err_)) {
      // do nothing if we already have an error,
      // so we can finish the sort process ASAP.
    } else if (!inited_) {
      *err_ = OB_NOT_INIT;
      LOG_WARN("not inited", K(*err_));
    } else if (OB_ISNULL(r1) || OB_ISNULL(r2)) {
      *err_ = OB_ERR_UNEXPECTED;
      LOG_WARN("r1 is null or r2 is null", K(bret), K(r1), K(r2));
    } else {
      int cmp = 0;
      for (int64_t i = 0; OB_SUCCESS == *err_ && 0 == cmp && i < cnt_; ++i) {
        obj_cmp_func_nullsafe cmp_func = sort_funcs_[i];
        cmp = cmp_func(r1->objs_[i], r2->objs_[i], sort_columns_[i].cs_type_, null_poses_[i]);
        if (cmp < 0) {
          bret = sort_columns_[i].is_ascending();
        } else if (cmp > 0) {
          bret = !sort_columns_[i].is_ascending();
        } else {
        }
      }  // end for
    }
    return bret;
  }

private:
  bool inited_;
  ObSortColumn* sort_columns_;
  ObCmpNullPos* null_poses_;
  obj_cmp_func_nullsafe* sort_funcs_;
  int64_t cnt_;
  int* err_;
};

struct ObBaseSort::TypelessRowComparer {
  explicit TypelessRowComparer(int* err) : inited_(false), sort_columns_(NULL), null_poses_(NULL), cnt_(0), err_(err)
  {}

  int init(ObIAllocator& allocator, const ObIArray<ObSortColumn>& sort_columns)
  {
    int ret = OB_SUCCESS;
    void* col_buf = NULL;
    void* null_pos_buf = NULL;
    int64_t col_buf_size = sizeof(ObSortColumn) * sort_columns.count();
    int64_t null_pos_size = sizeof(ObCmpNullPos) * sort_columns.count();

    if (col_buf_size <= 0) {
      // do nothing
    } else if (OB_ISNULL(col_buf = allocator.alloc(col_buf_size)) ||
               OB_ISNULL(null_pos_buf = allocator.alloc(null_pos_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      sort_columns_ = static_cast<ObSortColumn*>(col_buf);
      null_poses_ = static_cast<ObCmpNullPos*>(null_pos_buf);
      for (int64_t i = 0; i < sort_columns.count(); i++) {
        sort_columns_[i] = sort_columns.at(i);
        null_poses_[i] = sort_columns.at(i).get_cmp_null_pos();
      }
      cnt_ = sort_columns.count();
      inited_ = true;
    }

    return ret;
  }

  bool operator()(const StrongTypeRow* r1, const StrongTypeRow* r2)
  {
    bool bret = false;
    if (OB_UNLIKELY(OB_SUCCESS != *err_)) {
      // do nothing
    } else if (!inited_) {
      *err_ = OB_NOT_INIT;
      LOG_WARN("not inited", K(*err_));
    } else if (OB_ISNULL(r1) || OB_ISNULL(r2)) {
      *err_ = OB_ERR_UNEXPECTED;
    } else {
      int cmp = 0;
      ObCompareCtx cmp_ctx;
      cmp_ctx.cmp_type_ = ObMaxType;
      cmp_ctx.cmp_cs_type_ = CS_TYPE_INVALID;
      cmp_ctx.is_null_safe_ = true;
      cmp_ctx.tz_off_ = INVALID_TZ_OFF;
      for (int64_t i = 0; OB_SUCCESS == *err_ && 0 == cmp && i < cnt_; i++) {
        cmp_ctx.null_pos_ = null_poses_[i];
        cmp = r1->objs_[i].compare(r2->objs_[i], cmp_ctx);
        if (cmp < 0) {
          bret = sort_columns_[i].is_ascending();
        } else if (cmp > 0) {
          bret = !sort_columns_[i].is_ascending();
        } else {
          // do nothing
        }
      }
    }
    return bret;
  }

private:
  bool inited_;
  ObSortColumn* sort_columns_;
  ObCmpNullPos* null_poses_;
  int64_t cnt_;
  int* err_;
};

int ObBaseSort::add_row(const common::ObNewRow& row, bool& need_sort)
{
  return add_row(row, need_sort, true);
}

int ObBaseSort::add_row(const common::ObNewRow& row, bool& need_sort, bool deep_copy /*=true*/)
{
  int ret = OB_SUCCESS;
  bool is_cur_block = false;
  need_sort = false;
  const ObNewRow* last_row = NULL;
  ObArenaAllocator* alloc = NULL;
  if (sort_array_.count() > 0) {
    if (OB_ISNULL(last_row = get_last_row())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null typed row", K(ret), K(last_row));
    } else {
      // do nothing
    }
  }

  if (OB_FAIL(check_block_row(row, last_row, is_cur_block))) {
    LOG_WARN("check if row is belong to cur block failed", K(ret));
  } else {
    alloc = is_cur_block ? cur_alloc_ : next_block_alloc_;
  }

  if (OB_SUCC(ret)) {
    ObNewRow* new_row = deep_copy ? nullptr : const_cast<ObNewRow*>(&row);
    if (deep_copy && OB_FAIL(ObPhyOperator::deep_copy_row(row, new_row, *alloc))) {
      LOG_WARN("deep copy a new row failed", K(ret));
    } else if (!is_cur_block) {
      need_sort = true;
      next_block_row_ = new_row;  // new row is next sort block row
    } else if (OB_FAIL(add_typed_row(*new_row, *alloc))) {
      LOG_WARN("failed to add typed row", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

// check row and last_row are belong to same sort block
// such as prefix column keys is (a,b)
// (1,2,3) and (1,2,4) are same block
int ObBaseSort::check_block_row(const ObNewRow& row, const ObNewRow* last_row, bool& is_cur_block)
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
      cmp = row.get_cell(idx).compare(
          last_row->get_cell(idx), sort_column->at(i).cs_type_, sort_column->at(i).get_cmp_null_pos());
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

int ObBaseSort::sort_rows()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObSortColumn>* sort_column = NULL;
  if (OB_ISNULL(sort_column = get_sort_columns())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sort column is null", K(ret));
  } else if (OB_ISNULL(cur_alloc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null cur_alloc_", K(ret), K(cur_alloc_));
  } else if (prefix_keys_pos_ > sort_column->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err, prefix_keys_pos should be smaller than column count",
        K(prefix_keys_pos_),
        K(sort_column->count()));
  } else if (0 == sort_array_.count()) {
    // do nothing
  } else if (OB_ISNULL(sort_array_.at(0)) || OB_ISNULL(sort_array_.at(0)->row_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sort_array_.at(0)), K(sort_array_.at(0)->row_));
  } else {
    SQL_ENG_LOG(DEBUG, "sort_row", "count", sort_array_.count());
    bool is_typed_sort = true;
    if (OB_FAIL(enable_typed_sort(prefix_sort_columns_, prefix_keys_pos_, *sort_array_.at(0)->row_, is_typed_sort))) {
      LOG_WARN("failed to test whether typed sort is enabled", K(ret));
    } else if (OB_FAIL(inner_sort_rows(is_typed_sort))) {
      LOG_WARN("failed to do inner sort rows", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObBaseSort::get_next_row(common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  const common::ObNewRow* tmp_row = nullptr;
  if (OB_FAIL(get_next_row(tmp_row))) {
    if (OB_ITER_END == ret) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tmp row is null", K(ret), K(row_array_pos_));
    }
  } else if (OB_FAIL(attach_row(*tmp_row, row))) {
    LOG_WARN("fail to get row", K(ret));
  }
  return ret;
}

int ObBaseSort::attach_row(const common::ObNewRow& src, common::ObNewRow& dst)
{
  int ret = OB_SUCCESS;
  if (src.get_count() > dst.count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array row count is invalid", K(ret), K(src.count_), K(dst.count_));
  } else {
    int64_t idx = 0;
    while (idx < src.get_count()) {
      dst.cells_[idx] = src.get_cell(idx);
      ++idx;
    }
  }
  return ret;
}

int ObBaseSort::dump(ObIMergeSort& merge_sort)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(merge_sort.dump_base_run(*this))) {
    LOG_WARN("fail to dump data", K(ret));
  }
  return ret;
}

int ObBaseSort::final_dump(ObIMergeSort& merge_sort)
{
  return dump(merge_sort);
}

int ObBaseSort::add_typed_row(const ObNewRow& row, ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  StrongTypeRow* strong_type_row = nullptr;
  void* buf = nullptr;
  if (row.is_invalid()) {
    // do nothing
  } else if (OB_ISNULL(buf = alloc.alloc(sizeof(StrongTypeRow)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for StrongTypeRow");
  } else if (FALSE_IT(strong_type_row = new (buf) StrongTypeRow())) {
    // do nothing
  } else if (FALSE_IT(strong_type_row->row_ = &row)) {
    // do nothing
  } else if (OB_FAIL(strong_type_row->init(alloc, prefix_sort_columns_.count()))) {
    LOG_WARN("failed to init strong type row", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < prefix_sort_columns_.count(); i++) {
      int64_t idx = prefix_sort_columns_.at(i).index_;
      strong_type_row->objs_[i] = row.get_cell(idx);
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(sort_array_.push_back(strong_type_row))) {
    LOG_WARN("failed to push back array", K(ret));
  }
  return ret;
}

const ObNewRow* ObBaseSort::get_last_row() const
{
  const ObNewRow* last_row = nullptr;
  int64_t last_pos = sort_array_.count() - 1;
  if (OB_ISNULL(sort_array_.at(last_pos)) || OB_ISNULL(sort_array_.at(last_pos)->row_)) {
    // do nothing
  } else {
    last_row = sort_array_.at(last_pos)->row_;
  }
  return last_row;
}

int ObBaseSort::inner_sort_rows(const bool is_typed_sort)
{
  int ret = OB_SUCCESS;

  int err = OB_SUCCESS;
  if (is_typed_sort) {
    TypedRowComparer comparer(&err);
    if (OB_FAIL(comparer.init(*cur_alloc_, prefix_sort_columns_))) {
      LOG_WARN("failed to init RowComparer", K(ret));
    } else {
      const StrongTypeRow** first_row = &sort_array_.at(0);
      std::sort(first_row, first_row + sort_array_.count(), comparer);
      if (OB_SUCCESS != err) {
        LOG_WARN("failed to do sort", K(ret));
      } else {
        // do nothing
      }
    }
  } else {
    TypelessRowComparer comparer(&err);
    if (OB_FAIL(comparer.init(*cur_alloc_, prefix_sort_columns_))) {
      LOG_WARN("failed to init RowComparer", K(ret));
    } else {
      const StrongTypeRow** first_row = &sort_array_.at(0);
      std::sort(first_row, first_row + sort_array_.count(), comparer);
      if (OB_SUCCESS != err) {
        LOG_WARN("failed to do sort", K(ret));
      } else {
        // do nothing
      }
    }
  }
  if (OB_SUCC(ret) && OB_SUCCESS != err) {
    ret = err;
  }
  return ret;
}

// Whether it is strongly typed
int ObBaseSort::enable_typed_sort(const common::ObIArray<ObSortColumn>& sort_columns, const int64_t prefix_pos,
    const common::ObNewRow& row, bool& is_typed_sort)
{
  UNUSED(sort_columns);
  UNUSED(prefix_pos);
  UNUSED(row);

  int ret = OB_SUCCESS;
  is_typed_sort = false;
  return ret;
}

int ObBaseSort::get_next_row(const ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  const StrongTypeRow* tmp_row = nullptr;
  if (row_array_pos_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row array pos is invalid", K(row_array_pos_), K(sort_array_.count()));
  } else if (row_array_pos_ >= sort_array_.count()) {
    row_array_pos_ = 0;
    sort_array_.reset();
    cur_alloc_->reset();
    if (NULL != next_block_row_) {
      std::swap(cur_alloc_, next_block_alloc_);

      if (OB_ISNULL(cur_alloc_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null cur_alloc_", K(ret), K(cur_alloc_));
      } else if (OB_FAIL(add_typed_row(*next_block_row_, *cur_alloc_))) {
        LOG_WARN("failed to add typed row", K(ret));
      } else {
        next_block_row_ = nullptr;
      }
    }
    if (OB_SUCC(ret)) {
      ret = OB_ITER_END;
    }
  } else if (OB_ISNULL(tmp_row = sort_array_.at(row_array_pos_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp row is null", K(ret), K(row_array_pos_));
  } else if (OB_ISNULL(tmp_row->row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", K(ret), K(tmp_row->row_));
  } else {
    row = tmp_row->row_;
    ++row_array_pos_;
  }
  return ret;
}

DEFINE_SERIALIZE(ObSortColumn)
{
  int ret = OB_SUCCESS;
  int32_t cs_type_int = static_cast<int32_t>(cs_type_);
  OB_UNIS_ENCODE(index_);
  OB_UNIS_ENCODE(cs_type_int);
  bool is_asc = true;
  if ((extra_info_ & ObSortColumnExtra::SORT_COL_ASC_BIT) > 0) {
    is_asc = true;
  } else {
    is_asc = false;
  }
  if ((extra_info_ & ObSortColumnExtra::SORT_COL_EXTRA_BIT) > 0) {
    OB_UNIS_ENCODE(extra_info_);
    BASE_SER((ObSortColumn, ObSortColumnExtra));
  } else {
    OB_UNIS_ENCODE(is_asc);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSortColumn)
{
  int ret = OB_SUCCESS;
  int32_t cs_type_int = 0;
  OB_UNIS_DECODE(index_);
  OB_UNIS_DECODE(cs_type_int);
  cs_type_ = static_cast<ObCollationType>(cs_type_int);
  uint8_t extra_char = 0;
  OB_UNIS_DECODE(extra_char);

  if (OB_SUCC(ret)) {
    extra_info_ = extra_char;
    if ((extra_char & ObSortColumnExtra::SORT_COL_EXTRA_BIT) > 0) {
      BASE_DESER((ObSortColumn, ObSortColumnExtra));
    } else {
      // do nothing
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSortColumn)
{
  int64_t len = 0;
  int32_t cs_type_int = static_cast<int32_t>(cs_type_);
  OB_UNIS_ADD_LEN(index_);
  OB_UNIS_ADD_LEN(cs_type_int);  // for cs_type_.

  OB_UNIS_ADD_LEN(extra_info_);

  if ((extra_info_ & ObSortColumnExtra::SORT_COL_EXTRA_BIT) > 0) {
    BASE_ADD_LEN((ObSortColumn, ObSortColumnExtra));
  }
  return len;
}

OB_SERIALIZE_MEMBER(ObSortColumnExtra, obj_type_, order_type_);
