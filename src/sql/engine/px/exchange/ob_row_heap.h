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

#ifndef __OB_SQL_PX_ROW_HEAP_H__
#define __OB_SQL_PX_ROW_HEAP_H__

#include "lib/container/ob_array.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "share/ob_errno.h"
#include <algorithm>

namespace oceanbase {
namespace common {
class ObNewRow;
}
namespace sql {

class ObRowComparer {
public:
  ObRowComparer() : columns_(NULL), rows_(NULL), ret_(common::OB_SUCCESS)
  {}
  ~ObRowComparer() = default;
  int init(const common::ObIArray<ObSortColumn>& columns, const common::ObIArray<const common::ObNewRow*>& rows);
  bool operator()(int64_t row_idx1, int64_t row_idx2);
  int get_ret() const
  {
    return ret_;
  }

protected:
  const common::ObIArray<ObSortColumn>* columns_;
  const common::ObIArray<const common::ObNewRow*>* rows_;
  int ret_;
};

class ObDatumRowCompare {
public:
  ObDatumRowCompare();
  int init(const ObIArray<ObSortFieldCollation>* sort_collations, const ObIArray<ObSortCmpFunc>* sort_cmp_funs,
      const common::ObIArray<const ObChunkDatumStore::StoredRow*>& rows);

  // compare function for quick sort.
  bool operator()(int64_t row_idx1, int64_t row_idx2);

  bool is_inited() const
  {
    return NULL != sort_collations_;
  }
  // interface required by ObBinaryHeap
  int get_error_code()
  {
    return ret_;
  }

  void reset()
  {
    this->~ObDatumRowCompare();
    new (this) ObDatumRowCompare();
  }
  int get_ret() const
  {
    return ret_;
  }

public:
  int ret_;
  const ObIArray<ObSortFieldCollation>* sort_collations_;
  const ObIArray<ObSortCmpFunc>* sort_cmp_funs_;
  const common::ObIArray<const ObChunkDatumStore::StoredRow*>* rows_;
};

class ObMaxDatumRowCompare {
public:
  ObMaxDatumRowCompare();
  int init(const ObIArray<ObSortFieldCollation>* sort_collations, const ObIArray<ObSortCmpFunc>* sort_cmp_funs,
      const common::ObIArray<const ObChunkDatumStore::LastStoredRow<>*>& rows);

  // compare function for quick sort.
  bool operator()(int64_t row_idx1, int64_t row_idx2);

  bool is_inited() const
  {
    return NULL != sort_collations_;
  }
  // interface required by ObBinaryHeap
  int get_error_code()
  {
    return ret_;
  }

  void reset()
  {
    this->~ObMaxDatumRowCompare();
    new (this) ObMaxDatumRowCompare();
  }
  int get_ret() const
  {
    return ret_;
  }

public:
  int ret_;
  const ObIArray<ObSortFieldCollation>* sort_collations_;
  const ObIArray<ObSortCmpFunc>* sort_cmp_funs_;
  const common::ObIArray<const ObChunkDatumStore::LastStoredRow<>*>* rows_;
};

/*
 * sort data which received from N channels. N decrease on channel EOF
 */
template <class COMPARE = ObRowComparer, class ROW = common::ObNewRow>
class ObRowHeap {
public:
  ObRowHeap<COMPARE, ROW>();
  ~ObRowHeap<COMPARE, ROW>();

  int init(int64_t capacity, const common::ObIArray<ObSortColumn>& sort_columns);
  int init(int64_t capacity, const ObIArray<ObSortFieldCollation>* sort_collations,
      const ObIArray<ObSortCmpFunc>* sort_cmp_funs);

  void set_capacity(int64_t capacity)
  {
    capacity_ = capacity;
  }
  void set_sort_columns(const common::ObIArray<ObSortColumn>& sort_columns)
  {
    sort_columns_ = &sort_columns;
  }
  int init();

  int push(const ROW* row);
  int pop(const ROW*& row);
  // only for clean heap and free memory
  int raw_pop(const ROW*& row);
  void shrink();
  int64_t writable_channel_idx() const
  {
    return writable_ch_idx_;
  }
  int64_t capacity() const
  {
    return capacity_;
  }
  int64_t count() const
  {
    return row_idx_.count();
  }

  void reset()
  {
    row_idx_.reset();
    row_arr_.reset();
  }

  TO_STRING_KV(K_(writable_ch_idx), K_(capacity), "count", count());

private:
  /* functions */
  /* variables */
  bool inited_;
  int64_t writable_ch_idx_;
  int64_t capacity_;
  const common::ObIArray<ObSortColumn>* sort_columns_;
  common::ObArray<int64_t> row_idx_;
  common::ObArray<const ROW*> row_arr_;
  COMPARE indexed_row_comparer_;
  DISALLOW_COPY_AND_ASSIGN(ObRowHeap);
};

template <class COMPARE, class ROW>
ObRowHeap<COMPARE, ROW>::ObRowHeap() : inited_(false), writable_ch_idx_(0), capacity_(0), sort_columns_(NULL)
{}

template <class COMPARE, class ROW>
ObRowHeap<COMPARE, ROW>::~ObRowHeap()
{}

template <class COMPARE, class ROW>
int ObRowHeap<COMPARE, ROW>::init(int64_t capacity, const common::ObIArray<ObSortColumn>& sort_columns)
{
  int ret = common::OB_SUCCESS;
  if (capacity <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid capacity", K(capacity), K(ret));
  } else if (OB_FAIL(row_idx_.reserve(capacity))) {
    SQL_ENG_LOG(WARN, "fail alloc mem", K(capacity), K(ret));
  } else if (OB_FAIL(row_arr_.prepare_allocate(capacity))) {
    SQL_ENG_LOG(WARN, "fail alloc mem", K(capacity), K(ret));
  } else if (OB_FAIL(indexed_row_comparer_.init(sort_columns, row_arr_))) {
    SQL_ENG_LOG(WARN, "fail init comparer", K(ret));
  } else {
    writable_ch_idx_ = 0;
    capacity_ = capacity;
    inited_ = true;
  }
  return ret;
}

template <class COMPARE, class ROW>
int ObRowHeap<COMPARE, ROW>::init(int64_t capacity, const ObIArray<ObSortFieldCollation>* sort_collations,
    const ObIArray<ObSortCmpFunc>* sort_cmp_funs)
{
  int ret = common::OB_SUCCESS;
  if (capacity <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid capacity", K(capacity), K(ret));
  } else if (OB_FAIL(row_idx_.reserve(capacity))) {
    SQL_ENG_LOG(WARN, "fail alloc mem", K(capacity), K(ret));
  } else if (OB_FAIL(row_arr_.prepare_allocate(capacity))) {
    SQL_ENG_LOG(WARN, "fail alloc mem", K(capacity), K(ret));
  } else if (OB_FAIL(indexed_row_comparer_.init(sort_collations, sort_cmp_funs, row_arr_))) {
    SQL_ENG_LOG(WARN, "fail init comparer", K(ret));
  } else {
    writable_ch_idx_ = 0;
    capacity_ = capacity;
    inited_ = true;
  }
  return ret;
}

template <class COMPARE, class ROW>
int ObRowHeap<COMPARE, ROW>::init()
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(sort_columns_)) {
    ret = common::OB_NOT_INIT;
  } else {
    ret = init(capacity_, *sort_columns_);
  }
  return ret;
}

// push N times when heap not full, after heap full must pop before push
template <class COMPARE, class ROW>
int ObRowHeap<COMPARE, ROW>::push(const ROW* row)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = common::OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(
                 writable_ch_idx_ < 0 || writable_ch_idx_ >= row_arr_.count() || capacity_ <= row_idx_.count())) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN,
        "invalid state",
        K_(writable_ch_idx),
        "row_arr_cnt",
        row_arr_.count(),
        K_(capacity),
        "row_idx_cnt",
        row_idx_.count(),
        K(ret));
  } else if (OB_FAIL(row_idx_.push_back(writable_ch_idx_))) {
    SQL_ENG_LOG(WARN, "fail push row", K_(writable_ch_idx), K(ret));
  } else if (OB_UNLIKELY(NULL != row_arr_.at(writable_ch_idx_))) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "expect NULL in row_arr", K_(writable_ch_idx), K(ret));
  } else {
    row_arr_.at(writable_ch_idx_++) = row;
    std::push_heap(&row_idx_.at(0), (&row_idx_.at(0)) + row_idx_.count(), indexed_row_comparer_);
    if (OB_FAIL(indexed_row_comparer_.get_ret())) {
      SQL_ENG_LOG(WARN, "fail do heap sort", K(ret));
    }
  }
  return ret;
}

template <class COMPARE, class ROW>
int ObRowHeap<COMPARE, ROW>::pop(const ROW*& row)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = common::OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (row_idx_.count() != capacity_ || 0 >= capacity_) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "can not pop any element before the heap is full", K(ret));
  } else if (FALSE_IT(std::pop_heap(&row_idx_.at(0), (&row_idx_.at(0)) + row_idx_.count(), indexed_row_comparer_))) {
  } else if (OB_FAIL(indexed_row_comparer_.get_ret())) {
    SQL_ENG_LOG(WARN, "fail do heap pop", K(ret));
  } else if (OB_FAIL(row_idx_.pop_back(writable_ch_idx_))) {
    SQL_ENG_LOG(WARN, "fail get a row", K(ret));
  } else if (OB_UNLIKELY(NULL == row_arr_.at(writable_ch_idx_))) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "NULL row unexpected", K_(writable_ch_idx), K(ret));
  } else {
    row = row_arr_.at(writable_ch_idx_);
    row_arr_.at(writable_ch_idx_) = NULL;  // reset it
  }
  return ret;
}

template <class COMPARE, class ROW>
int ObRowHeap<COMPARE, ROW>::raw_pop(const ROW*& row)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = common::OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(row_idx_.pop_back(writable_ch_idx_))) {
    SQL_ENG_LOG(WARN, "fail get a row", K(ret));
  } else if (OB_UNLIKELY(NULL == row_arr_.at(writable_ch_idx_))) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "NULL row unexpected", K_(writable_ch_idx), K(ret));
  } else {
    row = row_arr_.at(writable_ch_idx_);
    row_arr_.at(writable_ch_idx_) = NULL;  // reset it
  }
  return ret;
}

template <class COMPARE, class ROW>
void ObRowHeap<COMPARE, ROW>::shrink()
{
  capacity_--;
  writable_ch_idx_++;
}

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_PX_ROW_HEAP_H__ */
//// end of header file
