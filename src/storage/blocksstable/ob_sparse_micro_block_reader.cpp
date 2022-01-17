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

#include "ob_sparse_micro_block_reader.h"
#include "ob_row_reader.h"
#include "ob_row_cache.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/row/ob_row.h"
#include "storage/ob_i_store.h"
#include "ob_column_map.h"
#include "ob_sparse_cell_reader.h"
#include "storage/ob_sstable_rowkey_helper.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {

/**
 * -----------------------------------ObSparseMicroBlockGetReader-------------------------
 */
ObSparseMicroBlockGetReader::ObSparseMicroBlockGetReader()
{}

ObSparseMicroBlockGetReader::~ObSparseMicroBlockGetReader()
{}

int ObSparseMicroBlockGetReader::get_row(const uint64_t tenant_id, const ObMicroBlockData& block_data,
    const common::ObStoreRowkey& rowkey, const ObColumnMap& column_map, const ObFullMacroBlockMeta& macro_meta,
    const storage::ObSSTableRowkeyHelper* rowkey_helper, storage::ObStoreRow& row)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_init(block_data))) {
    STORAGE_LOG(WARN, "failed to inner init, ", K(ret), K(block_data));
  } else {
    const char* row_buf = NULL;
    int64_t row_len = 0;
    ObSparseRowReader row_reader;
    if (OB_FAIL(locate_row(rowkey, rowkey_helper, macro_meta.schema_->column_type_array_, row_buf, row_len))) {
      if (OB_BEYOND_THE_RANGE != ret) {
        STORAGE_LOG(WARN, "failed to locate row, ", K(ret), K(rowkey));
      }
    } else if (OB_FAIL(row_reader.read_row(row_buf, row_len, 0, column_map, allocator_, row, FLAT_ROW_STORE))) {
      STORAGE_LOG(WARN, "Fail to read row, ", K(ret), K(rowkey));
    } else {
      STORAGE_LOG(DEBUG, "Success to read row", K(rowkey), K(row));
    }
  }
  return ret;
}

int ObSparseMicroBlockGetReader::get_row(const uint64_t tenant_id, const ObMicroBlockData& block_data,
    const common::ObStoreRowkey& rowkey, const ObFullMacroBlockMeta& macro_meta,
    const storage::ObSSTableRowkeyHelper* rowkey_helper, storage::ObStoreRow& row)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_init(block_data))) {
    STORAGE_LOG(WARN, "failed to inner init, ", K(ret), K(block_data));
  } else {
    const char* row_buf = NULL;
    int64_t row_len = 0;
    int64_t pos = 0;
    if (OB_FAIL(locate_row(rowkey, rowkey_helper, macro_meta.schema_->column_type_array_, row_buf, row_len))) {
      if (OB_BEYOND_THE_RANGE != ret) {
        STORAGE_LOG(WARN, "failed to locate row, ", K(ret), K(rowkey));
      }
    } else {
      ObSparseRowReader row_reader;
      if (OB_FAIL(row_reader.read_full_row(
              row_buf, row_len, pos, macro_meta.schema_->column_type_array_, allocator_, row))) {
        STORAGE_LOG(WARN, "failed to read full row", K(ret), K(rowkey), K(pos), K(row_len));
      } else {
        STORAGE_LOG(DEBUG, "success to read full row", K(ret), K(row), K(pos), K(row_len));
      }
    }
  }
  return ret;
}

int ObSparseMicroBlockGetReader::exist_row(const uint64_t tenant_id, const ObMicroBlockData& block_data,
    const common::ObStoreRowkey& rowkey, const ObFullMacroBlockMeta& macro_meta,
    const storage::ObSSTableRowkeyHelper* rowkey_helper, bool& exist, bool& found)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_init(block_data))) {
    STORAGE_LOG(WARN, "failed to inner init, ", K(ret), K(block_data));
  } else {
    const char* row_buf = NULL;
    int64_t row_len = 0;
    exist = false;
    found = false;
    if (OB_FAIL(locate_row(rowkey, rowkey_helper, macro_meta.schema_->column_type_array_, row_buf, row_len))) {
      if (OB_BEYOND_THE_RANGE == ret) {
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "failed to locate row, ", K(ret), K(rowkey));
      }
    } else {
      const ObRowHeader* row_header = reinterpret_cast<const ObRowHeader*>(row_buf);
      exist = ObActionFlag::OP_DEL_ROW != row_header->get_row_flag();
      found = true;
    }
  }
  return ret;
}

int ObSparseMicroBlockGetReader::locate_row(const common::ObStoreRowkey& rowkey,
    const storage::ObSSTableRowkeyHelper* rowkey_helper, const common::ObObjMeta* cols_type, const char*& row_buf,
    int64_t& row_len)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_cnt = rowkey.get_obj_cnt();
  const ObObj* rowkey_obj = rowkey.get_obj_ptr();
  int32_t high = header_->row_count_ - 1;
  int32_t low = 0;
  int32_t middle = 0;
  int32_t cmp_result = 0;
  int64_t pos = 0;
  bool found = false;
  ObObj cell[rowkey_cnt];
  ObSparseRowReader row_reader;
  ObSparseCellReader cell_reader;

  // binary search
  while (OB_SUCC(ret) && low <= high) {
    middle = (low + high) >> 1;
    row_buf = data_begin_ + index_data_[middle];
    row_len = index_data_[middle + 1] - index_data_[middle];
    pos = 0;
    cmp_result = 0;
    cell_reader.reset();  // reset cell reader
    if (OB_FAIL(row_reader.setup_row(row_buf, row_len, pos, 0))) {
      STORAGE_LOG(WARN, "failed to setup row", K(ret), K(pos), K(row_len));
    } else if (OB_FAIL(cell_reader.init(row_buf + row_reader.get_pos(), row_len, &allocator_))) {
      STORAGE_LOG(WARN, "failed to init CellReader", K(ret), K(pos), K(row_len));
    }
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp_result && i < rowkey_cnt; ++i) {
      if (OB_FAIL(cell_reader.read_cell(cell[i]))) {
        STORAGE_LOG(WARN, "Fail to read column, ", K(ret), K(rowkey_cnt), K(row_len), K(pos), K(i));
      } else if (OB_NOT_NULL(rowkey_helper)) {
        cell[i].set_meta_type(cols_type[i]);
        if (OB_FAIL(rowkey_helper->compare_rowkey_obj(i, cell[i], rowkey_obj[i], cmp_result))) {
          STORAGE_LOG(ERROR, "Fail to compare column, ", K(ret), K(rowkey_cnt), K(row_len), K(pos), K(i));
        }
      } else {
        cell[i].set_meta_type(cols_type[i]);
        cmp_result = cell[i].compare(rowkey_obj[i], common::CS_TYPE_INVALID);
      }
    }

    if (OB_SUCC(ret)) {
      if (cmp_result > 0) {
        high = middle - 1;
      } else if (cmp_result < 0) {
        low = middle + 1;
      } else {
        // equal
        high = middle - 1;
        found = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (found) {
      row_buf = data_begin_ + index_data_[low];
      row_len = index_data_[low + 1] - index_data_[low];
      row_idx_ = low;
      // When the located first row is MagicRow, it means that Rowkey does not exist
      if (OB_UNLIKELY(reinterpret_cast<const ObRowHeader*>(row_buf)->get_row_multi_version_flag().is_magic_row())) {
        ret = OB_BEYOND_THE_RANGE;
      }
    } else {
      ret = OB_BEYOND_THE_RANGE;
    }
  }
  return ret;
}

int ObSparseMicroBlockGetReader::check_row_locked(memtable::ObIMvccCtx& ctx,
    const transaction::ObTransStateTableGuard& trans_table_guard, const transaction::ObTransID& read_trans_id,
    const ObMicroBlockData& block_data, const common::ObStoreRowkey& rowkey, const ObFullMacroBlockMeta& macro_meta,
    const storage::ObSSTableRowkeyHelper* rowkey_helper, ObStoreRowLockState& lock_state)
{
  int ret = OB_SUCCESS;
  ObSparseRowReader row_reader;
  if (OB_FAIL(check_row_locked_(&row_reader,
          ctx,
          trans_table_guard,
          read_trans_id,
          block_data,
          rowkey,
          macro_meta,
          rowkey_helper,
          lock_state))) {
    STORAGE_LOG(WARN, "failed to check row locked", K(ret), K(block_data), K(read_trans_id));
  }
  return ret;
}

/***************               ObSparseMicroBlockReader              ****************/
ObSparseMicroBlockReader::ObSparseMicroBlockReader()
    : header_(NULL), data_begin_(NULL), data_end_(NULL), index_data_(NULL)
{
  reader_ = &sparse_row_reader_;
}

ObSparseMicroBlockReader::~ObSparseMicroBlockReader()
{
  reset();
  reader_ = NULL;
}

void ObSparseMicroBlockReader::reset()
{
  ObIMicroBlockReader::reset();
  header_ = NULL;
  data_begin_ = NULL;
  data_end_ = NULL;
  index_data_ = NULL;
  allocator_.reset();
  if (OB_NOT_NULL(reader_)) {
    reader_->reset();
  }
}

int ObSparseMicroBlockReader::init(const ObMicroBlockData& block_data, const ObColumnMap* column_map,
    const ObRowStoreType out_type /* = FLAT_ROW_STORE*/)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  }
  if (OB_NOT_NULL(column_map) && !column_map->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "column_map is invalid", K(ret), K(column_map));
  } else if (OB_FAIL(base_init(block_data))) {
    STORAGE_LOG(WARN, "fail to init, ", K(ret));
  } else {
    if (OB_ISNULL(reader_)) {
      reader_ = &sparse_row_reader_;
    }
    column_map_ = column_map;
    output_row_type_ = out_type;
    is_inited_ = true;
  }
  return ret;
}

int ObSparseMicroBlockReader::get_row(const int64_t index, ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(reader_)) {
    reader_->reuse_allocator();
  }
  if (OB_FAIL(get_row_impl(index, row))) {
    STORAGE_LOG(WARN, "get row failed", K(ret), K(index));
  }
  return ret;
}

OB_INLINE int ObSparseMicroBlockReader::get_row_impl(const int64_t index, storage::ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init reader first, ", K(ret));
  } else if (OB_UNLIKELY(index >= end() || !row.row_val_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(row.row_val_));
  } else if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row reader is null", K(ret), K(reader_));
  } else {
    if (OB_NOT_NULL(column_map_)) {  // use columnMap
      if (OB_FAIL(reader_->read_row(data_begin_,
              index_data_[index + 1],
              index_data_[index],
              *column_map_,
              allocator_,
              row,
              output_row_type_))) {
        STORAGE_LOG(WARN, "row reader fail to read row by column map", K(ret), K(index));
      } else {
        STORAGE_LOG(DEBUG, "read sparse row", K(row), K(index), K(output_row_type_));
      }
    } else if (OB_FAIL(reader_->read_full_row(
                   data_begin_, index_data_[index + 1], index_data_[index], nullptr, allocator_, row))) {
      STORAGE_LOG(WARN, "row reader fail to read full sparse row.", K(ret), K(index));
    } else {
      STORAGE_LOG(DEBUG, "read full sparse row", K(row), K(index));
    }
  }
  return ret;
}

int ObSparseMicroBlockReader::get_rows(const int64_t begin_index, const int64_t end_index, const int64_t row_capacity,
    storage::ObStoreRow* rows, int64_t& row_count)
{
  int ret = OB_SUCCESS;
  row_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY((begin_index == end_index) ||
                         (begin_index < end_index && !(begin_index >= begin() && end_index <= end())) ||
                         (begin_index > end_index && !(end_index >= begin() - 1 && begin_index <= end() - 1)) ||
                         NULL == rows || row_capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(ret),
        K(begin_index),
        K(end_index),
        K(begin()),
        K(end()),
        KP(rows),
        K(row_capacity));
  } else if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row reader is null", K(ret), K(reader_));
  } else {
    int64_t row_pos = 0;
    const int64_t step = begin_index < end_index ? 1 : -1;
    const int64_t out_column_count = column_map_->get_request_count();
    reader_->reuse_allocator();
    for (int64_t index = begin_index; OB_SUCC(ret) && index != end_index && row_pos < row_capacity; index += step) {
      ObStoreRow& cur_row = rows[row_pos];
      if (OB_ISNULL(cur_row.row_val_.cells_) || cur_row.capacity_ < out_column_count) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid argument", K(ret), K(cur_row.capacity_), K(out_column_count));
      } else if (OB_FAIL(get_row_impl(index, cur_row))) {
        STORAGE_LOG(WARN, "fail to get row", K(ret), K(row_pos), "obj_count", out_column_count);
      } else {
        ++row_pos;
      }
    }

    if (OB_SUCC(ret)) {
      row_count = row_pos;
    }
  }
  return ret;
}

int ObSparseMicroBlockReader::base_init(const ObMicroBlockData& block_data)
{
  int ret = OB_SUCCESS;
  const char* buf = NULL;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "reader already inited, ", K(ret));
  } else if (OB_UNLIKELY(!block_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "argument is invalid", K(ret), K(block_data));
  } else {
    buf = block_data.get_buf();
    header_ = reinterpret_cast<const ObMicroBlockHeader*>(buf);
    data_begin_ = buf + header_->header_size_;
    data_end_ = buf + header_->row_index_offset_;
    index_data_ = reinterpret_cast<const int32_t*>(buf + header_->row_index_offset_);
    end_ = header_->row_count_;
  }
  return ret;
}

template <typename ReaderType>
class PreciseCompareSparse {
public:
  PreciseCompareSparse(int& ret, bool& equal, ReaderType* reader, const char* data_begin, const int32_t* index_data,
      const ObColumnMap* column_map, const int64_t compare_column_count)
      : ret_(ret),
        equal_(equal),
        reader_(reader),
        data_begin_(data_begin),
        index_data_(index_data),
        column_map_(column_map),
        compare_column_count_(compare_column_count)
  {}
  ~PreciseCompareSparse()
  {}
  inline bool operator()(const int64_t row_idx, const ObStoreRowkey& rowkey)
  {
    return compare(row_idx, rowkey, true);
  }
  inline bool operator()(const ObStoreRowkey& rowkey, const int64_t row_idx)
  {
    return compare(row_idx, rowkey, false);
  }

private:
  inline bool compare(const int64_t row_idx, const ObStoreRowkey& rowkey, const bool lower_bound)
  {
    bool bret = false;
    int& ret = ret_;
    int32_t compare_result = 0;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(reader_) || OB_ISNULL(data_begin_) || OB_ISNULL(index_data_) || compare_column_count_ < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument",
          K(ret),
          KP(reader_),
          KP(data_begin_),
          KP(index_data_),
          KP(column_map_),
          K(compare_column_count_));
    } else if (OB_FAIL(reader_->compare_meta_rowkey(rowkey,
                   column_map_,
                   compare_column_count_,
                   data_begin_,
                   index_data_[row_idx + 1],
                   index_data_[row_idx],
                   compare_result))) {
      LOG_WARN("fail to compare rowkey", K(ret));
    } else {
      bret = lower_bound ? compare_result < 0 : compare_result > 0;
      // binary search will keep searching after find the first equal item,
      // if we need the equal reuslt, must prevent it from being modified again
      if (0 == compare_result && !equal_) {
        equal_ = true;
      }
    }
    return bret;
  }

private:
  int& ret_;
  bool& equal_;
  ReaderType* reader_;
  const char* data_begin_;
  const int32_t* index_data_;
  const ObColumnMap* column_map_;
  int64_t compare_column_count_;
};

int ObSparseMicroBlockReader::find_bound(const common::ObStoreRowkey& key, const bool lower_bound,
    const int64_t begin_idx, const int64_t end_idx, int64_t& row_idx, bool& equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  row_idx = ObIMicroBlockReader::INVALID_ROW_INDEX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_UNLIKELY(!key.is_valid() || begin_idx < begin() || end_idx > end())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(begin_idx), K(begin()), K(end_idx), K(end()));
  } else {
    PreciseCompareSparse<ObIRowReader> flat_compare(
        ret, equal, reader_, data_begin_, index_data_, column_map_, key.get_obj_cnt());
    ObRowIndexIterator begin_iter(begin_idx);
    ObRowIndexIterator end_iter(end_idx);
    ObRowIndexIterator found_iter;
    if (lower_bound) {
      found_iter = std::lower_bound(begin_iter, end_iter, key, flat_compare);
    } else {
      found_iter = std::upper_bound(begin_iter, end_iter, key, flat_compare);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to lower bound rowkey", K(ret));
    } else {
      row_idx = *found_iter;
    }
  }
  return ret;
}

int ObSparseMicroBlockReader::get_row_count(int64_t& row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    row_count = header_->row_count_;
  }
  return ret;
}

int ObSparseMicroBlockReader::get_row_header(const int64_t row_idx, const ObRowHeader*& row_header)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "reader not init", K(ret));
  } else if (OB_UNLIKELY(row_idx >= end())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "id is NULL, ", K(ret), K(row_idx));
  } else if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row reader is null", K(ret), K(reader_));
  } else if (OB_FAIL(reader_->setup_row(data_begin_, index_data_[row_idx + 1], index_data_[row_idx], INT64_MAX))) {
    STORAGE_LOG(WARN, "failed to setup row", K(ret), K(row_idx));
  } else if (OB_FAIL(reader_->get_row_header(row_header))) {
    STORAGE_LOG(WARN, "failed to get row header", K(ret), K(row_idx));
  }
  return ret;
}

int ObSparseMicroBlockReader::get_multi_version_info(const int64_t row_idx, const int64_t version_column_idx,
    const int64_t sql_sequence_idx, storage::ObMultiVersionRowFlag& flag, transaction::ObTransID& trans_id,
    int64_t& trans_version, int64_t& sql_sequence)
{
  int ret = OB_SUCCESS;
  const ObRowHeader* row_header = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(row_idx < begin() || row_idx > end() || version_column_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_idx), K(version_column_idx), K(header_->row_count_));
  } else if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row reader is null", K(ret), K(reader_));
  } else {  // get trans_id and setup row
    reader_->reset();
    if (OB_FAIL(
            reader_->setup_row(data_begin_, index_data_[row_idx + 1], index_data_[row_idx], INT64_MAX, &trans_id))) {
      LOG_WARN("fail to setup sparse row", K(ret));
    } else if (OB_FAIL(reader_->get_row_header(row_header))) {
      LOG_WARN("fail to get row header", K(ret));
    } else {
      flag.flag_ = row_header->get_row_type_flag();
    }
  }
  if (OB_SUCC(ret)) {
    if (!flag.is_uncommitted_row()) {  // get trans_version for committed row
      sql_sequence = 0;
      ObObjMeta version_column_meta;
      version_column_meta.set_int();
      ObObj version_cell;
      if (OB_FAIL(reader_->read_column(version_column_meta, allocator_, version_column_idx, version_cell))) {
        LOG_WARN("fail to read version column", K(ret));
      } else if (OB_FAIL(version_cell.get_int(trans_version))) {
        LOG_WARN("fail to convert version cell to int", K(ret), K(version_cell));
      } else {
        trans_version = -trans_version;
      }
    } else {  // get sql_sequence for uncommitted row
      trans_version = INT64_MAX;
      if (sql_sequence_idx < 0) {  // for compat Old Version SStable without sql_sequence column
        sql_sequence = 0;
      } else {  // have sql_sequence column
        ObObjMeta sql_sequence_column_meta;
        sql_sequence_column_meta.set_int();
        ObObj sql_sequence_cell;
        if (OB_FAIL(reader_->read_column(sql_sequence_column_meta, allocator_, sql_sequence_idx, sql_sequence_cell))) {
          LOG_WARN("fail to read version column", K(ret));
        } else if (OB_FAIL(sql_sequence_cell.get_int(sql_sequence))) {
          LOG_ERROR("fail to convert sql msequence cell to int", K(ret), K(sql_sequence_cell));
        } else {
          sql_sequence = -sql_sequence;
        }
      }
    }
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
