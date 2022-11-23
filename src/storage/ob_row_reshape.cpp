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

#include "storage/ob_row_reshape.h"
#include "common/sql_mode/ob_sql_mode_utils.h"

namespace oceanbase {
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace storage {

ObRowReshape::ObRowReshape()
  : row_reshape_cells_len_(0),
    row_reshape_cells_(nullptr),
    char_only_(false),
    binary_buffer_len_(0),
    binary_buffer_ptr_(nullptr),
    binary_len_array_()
{
}

int ObRowReshapeUtil::malloc_rows_reshape(
    ObIAllocator &work_allocator,
    const ObColDescIArray &col_descs,
    const int64_t row_count,
    const ObRelativeTable &table,
    ObRowReshape *&row_reshape_ins)
{
  int ret = OB_SUCCESS;
  int64_t binary_buffer_len = 0;
  ObSEArray<std::pair<int32_t, int32_t>, common::OB_ROW_DEFAULT_COLUMNS_COUNT> binary_len_array;

  if (col_descs.count() <= 0 || nullptr != row_reshape_ins || !table.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(col_descs), K(row_reshape_ins), K(table));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
      const ObColDesc &col_desc = col_descs.at(i);
      if (col_desc.col_type_.is_binary()) {
        int32_t data_length = 0;
        if (OB_FAIL(table.get_column_data_length(col_desc.col_id_, data_length))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column data length fail", K(ret), K(col_desc));
        } else if (OB_FAIL(binary_len_array.push_back(std::make_pair(i, data_length)))) {
          LOG_WARN("fail to push element into row_reshape_ins", K(ret), K(i), K(data_length));
        } else {
          if (OB_UNLIKELY(data_length < 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("wrong data length", K(col_desc.col_id_), K(data_length), K(ret));
          } else {
            binary_buffer_len += data_length;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    void *ptr = NULL;
    int64_t num = col_descs.count();
    int64_t reshape_size = sizeof(ObRowReshape);
    int64_t cell_size = sizeof(ObObj) * num;
    int64_t total_size = reshape_size + cell_size + binary_buffer_len;
    if (OB_ISNULL(ptr = work_allocator.alloc(row_count * total_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to malloc temp row cells", K(ret));
    } else {
      row_reshape_ins = new (ptr) ObRowReshape[row_count];
      int64_t reshape_sizes = row_count * reshape_size;
      int64_t remain_size = cell_size + binary_buffer_len;
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
        int64_t offset = reshape_sizes + i * remain_size;
        void *cell_ptr = static_cast<char *>(ptr) + offset;
        row_reshape_ins[i].row_reshape_cells_len_ = num;
        row_reshape_ins[i].row_reshape_cells_ = new (cell_ptr) ObObj[num]();
        row_reshape_ins[i].binary_buffer_len_ = binary_buffer_len;
        if (binary_buffer_len > 0) {
          row_reshape_ins[i].binary_buffer_ptr_ = static_cast<char *>(cell_ptr) + cell_size;
        }
        if (binary_len_array.count() > 0) {
          if (OB_FAIL(row_reshape_ins[i].binary_len_array_.assign(binary_len_array))) {
            LOG_WARN("failed to assign binary_len_array", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObRowReshapeUtil::malloc_rows_reshape_if_need(
    ObIAllocator &work_allocator,
    const ObColDescIArray &col_descs,
    const int64_t row_count,
    const ObRelativeTable &table,
    const ObSQLMode sql_mode,
    ObRowReshape *&row_reshape_ins)
{
  int ret = OB_SUCCESS;
  bool char_binary_exists = false;
  bool has_empty_string = false;
  bool char_only = true;
  int64_t binary_buffer_len = 0;
  ObSEArray<std::pair<int32_t, int32_t>, common::OB_ROW_DEFAULT_COLUMNS_COUNT> binary_len_array;

  if (OB_UNLIKELY(col_descs.count() <= 0 || NULL != row_reshape_ins || !table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(col_descs), K(row_reshape_ins), K(table));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
    const ObColDesc &col_desc = col_descs.at(i);
    if (col_desc.col_type_.is_fixed_len_char_type() || col_desc.col_type_.is_binary()) {
      char_binary_exists = true;
      if (col_desc.col_type_.is_binary()) {
        char_only = false;
        int32_t data_length = 0;
        if (OB_FAIL(table.get_column_data_length(col_desc.col_id_, data_length))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column data length fail", K(ret), K(col_desc));
        } else if (OB_FAIL(binary_len_array.push_back(std::make_pair(i, data_length)))) {
          LOG_WARN("fail to push element into row_reshape_ins", K(ret), K(i), K(data_length));
        } else {
          if (OB_UNLIKELY(data_length < 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("wrong data length", K(col_desc.col_id_), K(data_length), K(ret));
          } else {
            binary_buffer_len += data_length;
          }
        }
      }
    } else if (is_oracle_compatible(sql_mode) && col_desc.col_type_.is_character_type()) {
      has_empty_string = true;
    }
  }
  if (OB_SUCC(ret) && (char_binary_exists || has_empty_string)) {
    void *ptr = NULL;
    int64_t num = col_descs.count();
    int64_t reshape_size = sizeof(ObRowReshape);
    int64_t cell_size = sizeof(ObObj) * num;
    int64_t total_size = reshape_size + cell_size + binary_buffer_len;
    if (OB_ISNULL(ptr = work_allocator.alloc(row_count * total_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to malloc temp row cells", K(ret));
    } else {
      row_reshape_ins = new (ptr) ObRowReshape[row_count];
      int64_t reshape_sizes = row_count * reshape_size;
      int64_t remain_size = cell_size + binary_buffer_len;
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
        int64_t offset = reshape_sizes + i * remain_size;
        row_reshape_ins[i].char_only_ = char_only;
        void *cell_ptr = static_cast<char *>(ptr) + offset;
        row_reshape_ins[i].row_reshape_cells_len_ = num;
        row_reshape_ins[i].row_reshape_cells_ = new (cell_ptr) ObObj[num]();
        row_reshape_ins[i].binary_buffer_len_ = binary_buffer_len;
        if (binary_buffer_len > 0) {
          row_reshape_ins[i].binary_buffer_ptr_ = static_cast<char *>(cell_ptr) + cell_size;
        }
        if (binary_len_array.count() > 0) {
          if (OB_FAIL(row_reshape_ins[i].binary_len_array_.assign(binary_len_array))) {
            LOG_WARN("failed to assign binary_len_array", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

void ObRowReshapeUtil::free_row_reshape(
    ObIAllocator &work_allocator,
    ObRowReshape *&row_reshape_ins,
    int64_t row_count)
{
  if (NULL != row_reshape_ins) {
    for (int64_t i = 0; i < row_count; i++) {
      row_reshape_ins[i].~ObRowReshape();
    }
    work_allocator.free(row_reshape_ins);
    row_reshape_ins = NULL;
  }
}

int ObRowReshapeUtil::need_reshape_table_row(
    const ObNewRow &row,
    ObRowReshape *row_reshape_ins,
    int64_t row_reshape_cells_count,
    ObSQLMode sql_mode,
    bool &need_reshape)
{
  int ret = OB_SUCCESS;
  need_reshape = false;
  if (!row.is_valid() || row_reshape_cells_count != row.get_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(row), K(row.count_), K(row_reshape_cells_count), K(ret));
  } else {
    if (NULL == row_reshape_ins) {
      // do not need reshape
    } else if (row_reshape_ins->char_only_) {
      if (OB_FAIL(need_reshape_table_row(row, row.get_count(), sql_mode, need_reshape))) {
        LOG_WARN("failed to check need reshape row", K(ret), K(row), K(sql_mode));
      }
    } else {
      need_reshape = true;  // with binary, we do not check it
    }
  }
  return ret;
}

int ObRowReshapeUtil::need_reshape_table_row(
    const ObNewRow &row,
    const int64_t column_cnt,
    ObSQLMode sql_mode,
    bool &need_reshape)
{
  int ret = OB_SUCCESS;
  need_reshape = false;
  if (!row.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row), K(row.count_));
  } else {
    ObString space_pattern;
    for (int64_t i = 0; !need_reshape && i < column_cnt; ++i) {
      const ObObj &cell = row.get_cell(i);
      if (cell.is_fixed_len_char_type()) {
        space_pattern = ObCharsetUtils::get_const_str(cell.get_collation_type(), ' ');
      }
      if (cell.is_fixed_len_char_type() && cell.get_string_len() >= space_pattern.length() &&
          0 == MEMCMP(cell.get_string_ptr() + cell.get_string_len() - space_pattern.length(),
                   space_pattern.ptr(),
                   space_pattern.length())) {
        need_reshape = true;
      } else if (is_oracle_compatible(sql_mode) && cell.is_character_type() && cell.get_string_len() == 0) {
        // Oracle compatibility mode: '' as null
        need_reshape = true;
        LOG_DEBUG("Pstor2", K(cell), K(cell.get_string()), K(need_reshape));
      } else if (cell.is_binary()) {
        need_reshape = true;
      }
    }
  }
  return ret;
}

int ObRowReshapeUtil::reshape_row(
    const ObNewRow &row,
    const int64_t column_cnt,
    ObRowReshape *row_reshape_ins,
    bool need_reshape,
    ObSQLMode sql_mode,
    ObStoreRow &tbl_row)
{
  int ret = OB_SUCCESS;
  if (column_cnt > row.get_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column cnt can not be larger than row column cnt", K(ret), K(column_cnt), K(row));
  } else if (!need_reshape) {
    tbl_row.row_val_ = row;
  } else if (OB_ISNULL(row_reshape_ins)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null reshape ptr", K(ret));
  } else if (OB_UNLIKELY(column_cnt > row_reshape_ins->row_reshape_cells_len_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column count", K(ret), K(column_cnt), K(row_reshape_ins->row_reshape_cells_len_));
  } else {
    int64_t binary_len_array_count = row_reshape_ins->binary_len_array_.count();
    ObDataBuffer data_buffer(row_reshape_ins->binary_buffer_ptr_, row_reshape_ins->binary_buffer_len_);
    for (int64_t i = 0, j = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      const ObObj &cell = row.get_cell(i);
      if (cell.is_binary()) {
        for (; j < binary_len_array_count; j++) {
          if (row_reshape_ins->binary_len_array_.at(j).first == i) {
            break;
          }
        }
        if (OB_UNLIKELY(binary_len_array_count <= j)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(binary_len_array_count), K(ret));
        } else {
          const char *str = cell.get_string_ptr();
          const int32_t len = cell.get_string_len();
          char *dest_str = NULL;
          const int32_t binary_len = row_reshape_ins->binary_len_array_.at(j++).second;
          if (binary_len > len) {
            if (OB_ISNULL(dest_str = (char *)(data_buffer.alloc(binary_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_ERROR("fail to alloc mem to binary", K(ret), K(i), K(j), K(binary_len));
            } else {
              char pad_char = '\0';
              MEMCPY(dest_str, str, len);
              MEMSET(dest_str + len, pad_char, binary_len - len);
            }
          } else if (binary_len == len) {
            dest_str = const_cast<char *>(str);
          } else if (binary_len < len) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("binary_len should be greater than len", K(ret), K(binary_len), K(len));
          }
          if (OB_SUCC(ret)) {
            // set_binary set both type_ and cs_type
            row_reshape_ins->row_reshape_cells_[i].set_binary(ObString(binary_len, dest_str));
          }
        }
      } else if (is_oracle_compatible(sql_mode) && cell.is_character_type() && cell.get_string_len() == 0) {
        // Oracle compatibility mode: '' as null
        LOG_DEBUG("reshape empty string to null", K(cell));
        row_reshape_ins->row_reshape_cells_[i].set_null();
      } else if (cell.is_fixed_len_char_type()) {
        const char *str = cell.get_string_ptr();
        int32_t len = cell.get_string_len();
        ObString space_pattern = ObCharsetUtils::get_const_str(cell.get_collation_type(), ' ');
        for (; len >= space_pattern.length(); len -= space_pattern.length()) {
          if (0 != MEMCMP(str + len - space_pattern.length(), space_pattern.ptr(), space_pattern.length())) {
            break;
          }
        }
        // need to set collation type
        row_reshape_ins->row_reshape_cells_[i].set_string(cell.get_type(), ObString(len, str));
        row_reshape_ins->row_reshape_cells_[i].set_collation_type(cell.get_collation_type());
      } else {
        row_reshape_ins->row_reshape_cells_[i] = cell;
      }
    }
    tbl_row.row_val_.cells_ = row_reshape_ins->row_reshape_cells_;
    tbl_row.row_val_.count_ = column_cnt;
  }
  return ret;
}

int ObRowReshapeUtil::reshape_table_rows(
    const ObNewRow *rows,
    ObRowReshape *row_reshape_ins,
    int64_t row_reshape_cells_count,
    ObStoreRow *tbl_rows,
    int64_t row_count,
    ObSQLMode sql_mode)
{
  int ret = OB_SUCCESS;
  bool need_reshape = false;
  if (row_count <= 0) {
    ret = OB_ERR_WRONG_VALUE_COUNT_ON_ROW;
    LOG_WARN("row count should be bigger than 0", K(row_count), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      if (OB_FAIL(need_reshape_table_row(rows[i], row_reshape_ins,
          row_reshape_cells_count, sql_mode, need_reshape))) {
        LOG_WARN("fail to check whether reshape is needed");
      } else if (OB_FAIL(reshape_row(rows[i], rows[i].get_count(), &row_reshape_ins[i],
          need_reshape, sql_mode, tbl_rows[i]))) {
        LOG_WARN("fail to reshape row", K(ret));
      }
    }
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase
