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

#ifndef OCEANBASE_COMMON_OB_ROW_
#define OCEANBASE_COMMON_OB_ROW_

#include "lib/container/ob_array.h"
#include "lib/utility/ob_hang_fatal_error.h"
#include "lib/json/ob_yson.h"
#include "common/row/ob_row_desc.h"
#include "common/rowkey/ob_rowkey.h"

namespace commontest
{
class ObRowTest_reset_test_Test;
}

namespace oceanbase
{
namespace common
{

class ObColumnInfo
{
public:
  int64_t index_;
  common::ObCollationType cs_type_;
  ObColumnInfo() : index_(common::OB_INVALID_INDEX), cs_type_(common::CS_TYPE_INVALID) {}
  virtual ~ObColumnInfo() {}
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV2(N_INDEX_ID, index_,
                N_COLLATION_TYPE, common::ObCharset::collation_name(cs_type_));
};

/**
 * @brief: wrap row pointer(an ObObj array) and row size
 */
class ObNewRow final
{
public:
  ObNewRow()
      :cells_(NULL),
       count_(0),
       projector_size_(0),
       projector_(NULL)
  {}
  ObNewRow(ObObj *cells, int64_t count)
      :cells_(cells),
       count_(count),
       projector_size_(0),
       projector_(NULL)
  {}

  void reset();
  OB_INLINE bool is_invalid() const
  {
    return NULL == cells_ || count_ <= 0 || (projector_size_ > 0 && NULL == projector_);
  }

  OB_INLINE bool is_valid() const
  {
    return !is_invalid();
  }

  OB_INLINE void assign(ObObj *ptr, const int64_t count)
  {
    cells_ = ptr;
    count_ = count;
  }
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObNewRow &src, char *buf, int64_t len, int64_t &pos);
  //According to the existing buffer to construct an ObNewRow, the memory format is:
  //The memory structure allocated by sizeof(ObNewRow) + ObNewRow.deep_copy()
  static int construct(char *buf, int64_t len, int64_t &pos, ObNewRow *&row);
  NEED_SERIALIZE_AND_DESERIALIZE;

  bool operator==(const ObNewRow &other) const;
  OB_INLINE int64_t get_count() const { return projector_size_ > 0 ? projector_size_ : count_; }
  const ObObj &get_cell(int64_t index) const
  {
    int64_t real_idx = index;
    if (projector_size_ > 0) {
      if (OB_ISNULL(projector_) || index >= projector_size_ || index < 0) {
        COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "index is invalid", K(index), K_(projector_size), K_(projector));
      } else {
        real_idx = projector_[index];
      }
    }
    if (OB_UNLIKELY(real_idx >= count_)) {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "real_idx is invalid", K_(count), K(real_idx));
    }
    return cells_[real_idx];
  }

  ObObj &get_cell(int64_t index)
  {
    int64_t real_idx = index;
    if (projector_size_ > 0) {
      if (OB_ISNULL(projector_) || index >= projector_size_ || index < 0) {
        COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "index is invalid", K(index), K_(projector_size), K_(projector));
        right_to_die_or_duty_to_live();
      } else {
        real_idx = projector_[index];
      }
    }
    if (real_idx >= count_) {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "real_idx is invalid", K_(count), K(real_idx));
      right_to_die_or_duty_to_live();
    }
    return cells_[real_idx];
  }

  int32_t get_project_idx(int64_t cell_idx) const
  {
    int32_t proj_idx = -1;
    if (0 <= cell_idx && cell_idx < count_) {
      if (OB_ISNULL(projector_)) {
        proj_idx = static_cast<int32_t>(cell_idx);
      } else {
        for (int32_t i = 0; i < projector_size_; i++) {
          if (projector_[i] == cell_idx) {
            proj_idx = i;
            break;
          }
        }
      }
    }
    return proj_idx;
  }

  TO_YSON_KV(OB_ID(row), to_cstring(*this));
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_NAME(N_ROW);
    J_COLON();
    (void)databuff_print_obj_array(buf, buf_len, pos, cells_, count_);
    J_COMMA();
    J_NAME(N_PROJECTOR);
    J_COLON();
    (void)databuff_print_obj_array(buf, buf_len, pos, projector_, projector_size_);
    J_OBJ_END();
    return pos;
  }
public:
  ObObj *cells_;
  int64_t count_;  // cells count
  int64_t projector_size_;
  int32_t *projector_;  // if projector is not NULL, the caller should use this projector to access cells_
};

template<typename AllocatorT>
int ob_write_row(AllocatorT &allocator, const ObNewRow &src, ObNewRow &dst)
{
  int ret = OB_SUCCESS;
  void *ptr1 = NULL;
  void *ptr2 = NULL;
  if (src.count_ <= 0) {
    dst.count_ = src.count_;
    dst.cells_ = NULL;
    dst.projector_size_ = 0;
    dst.projector_ = NULL;
  } else if (OB_ISNULL(ptr1 = allocator.alloc(sizeof(ObObj) * src.count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    _OB_LOG(ERROR, "out of memory");
  } else if (NULL != src.projector_
      && OB_ISNULL(ptr2 = allocator.alloc(sizeof(int32_t) * src.projector_size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    _OB_LOG(ERROR, "out of memory");
  } else {
    if (NULL != src.projector_) {
      MEMCPY(ptr2, src.projector_, sizeof(int32_t) * src.projector_size_);
    }
    ObObj *objs = new(ptr1) ObObj[src.count_]();
    for (int64_t i = 0; OB_SUCC(ret) && i < src.count_; ++i) {
      if (OB_FAIL(ob_write_obj(allocator, src.cells_[i], objs[i]))) {
        _OB_LOG(WARN, "copy ObObj error, row=%s, i=%ld, ret=%d",
            to_cstring(src.cells_[i]), i, ret);
      }
    }
    if (OB_SUCC(ret)) {
      dst.count_ = src.count_;
      dst.cells_ = objs;
      dst.projector_size_ = src.projector_size_;
      dst.projector_ = (NULL != src.projector_) ? static_cast<int32_t *>(ptr2) : NULL;
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != ptr1) {
      allocator.free(ptr1);
      ptr1 = NULL;
    }
    if (NULL != ptr2) {
      allocator.free(ptr2);
      ptr2 = NULL;
    }
  }
  return ret;
}

template<typename AllocatorT>
int ob_write_row_by_projector(AllocatorT &allocator, const ObNewRow &src, ObNewRow &dst)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  int64_t len = sizeof(ObObj) * src.get_count();
  if (src.count_ <= 0) {
    dst.reset();
  } else if (OB_ISNULL(ptr = allocator.alloc(len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "no memory to create row cells", K(ret), K(len));
  } else {
    dst.cells_ = new(ptr) ObObj[src.get_count()];
    dst.count_ = src.get_count();
    dst.projector_ = NULL;
    dst.projector_size_ = 0;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src.get_count(); ++i) {
    if (OB_FAIL(ob_write_obj(allocator, src.get_cell(i), dst.cells_[i]))) {
      OB_LOG(WARN, "write obj failed", K(ret), K(src.get_cell(i)));
    }
  }
  return ret;
}

template<typename AllocatorT>
int ob_create_row(AllocatorT &allocator, int64_t col_count, ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "row_count is invalid", K(ret), K(col_count));
  } else {
    void *row_buf = NULL;
    size_t row_size = sizeof(ObNewRow);
    int64_t row_buf_len = row_size + col_count * sizeof(ObObj);
    if (OB_ISNULL(row_buf = allocator.alloc(row_buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate row buffer failed", K(ret), K(row_buf_len));
    } else {
      void *cells_buf = static_cast<char*>(row_buf) + row_size;
      row = new(row_buf) ObNewRow();
      row->cells_ = new(cells_buf) ObObj[col_count];
      row->count_ = col_count;
      row->projector_ = NULL;
      row->projector_size_ = 0;
    }
  }
  return ret;
}

template<typename AllocatorT>
int ob_create_row(AllocatorT &allocator, int64_t col_count, ObNewRow &row)
{
  int ret = OB_SUCCESS;
  row.reset();
  if (OB_UNLIKELY(col_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "row_count is invalid", K(ret), K(col_count));
  } else {
    void *cell_buf = NULL;
    int64_t cell_buf_len = col_count * sizeof(ObObj);
    if (OB_ISNULL(cell_buf = allocator.alloc(cell_buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate row buffer failed", K(ret), K(cell_buf_len));
    } else {
      row.cells_ = new(cell_buf) ObObj[col_count];
      row.count_ = col_count;
      row.projector_ = NULL;
      row.projector_size_ = 0;
    }
  }
  return ret;
}

template<typename AllocatorT>
int ob_create_rows(AllocatorT &allocator, int64_t row_count, int64_t col_count, ObNewRow *&rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_count <= 0 || row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "col count or row count is invalid", K(ret), K(col_count), K(row_count));
  } else {
    void *rows_buf = nullptr;
    const size_t row_size = sizeof(ObNewRow);
    const size_t col_size = col_count * sizeof(ObObj);
    const int64_t rows_buf_len = (row_size + col_size) * row_count;
    if (OB_ISNULL(rows_buf = allocator.alloc(rows_buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "Failed to allocate row buffer", K(ret), K(rows_buf_len));
    } else {
      char *row_buf = static_cast<char*>(rows_buf);
      char *col_buf = row_buf + row_size * row_count;
      for (int64_t i = 0; i < row_count; ++i) {
        ObNewRow *row = new(row_buf) ObNewRow();
        row->cells_ = new(col_buf) ObObj[col_count];
        row->count_ = col_count;
        row->projector_ = nullptr;
        row->projector_size_ = 0;
        row_buf = row_buf + row_size;
        col_buf = col_buf + col_size;
      }
      rows = reinterpret_cast<ObNewRow *>(rows_buf);
    }
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_ROW_ */
