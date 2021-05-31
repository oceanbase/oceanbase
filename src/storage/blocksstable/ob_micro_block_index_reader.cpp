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

#include "ob_micro_block_index_reader.h"

namespace oceanbase {
using namespace common;
namespace blocksstable {

ObMicroBlockIndexReader::ObMicroBlockIndexReader()
    : row_reader_(nullptr),
      column_type_array_(nullptr),
      micro_indexes_(nullptr),
      endkey_stream_(nullptr),
      mark_deletion_stream_(nullptr),
      delta_array_(nullptr),
      block_count_(0),
      row_key_column_cnt_(0),
      data_base_offset_(0),
      is_inited_(false)
{}

ObMicroBlockIndexReader::~ObMicroBlockIndexReader()
{}

void ObMicroBlockIndexReader::reset()
{
  row_reader_ = nullptr;
  column_type_array_ = nullptr;
  micro_indexes_ = nullptr;
  endkey_stream_ = nullptr;
  mark_deletion_stream_ = nullptr;
  block_count_ = 0;
  row_key_column_cnt_ = 0;
  data_base_offset_ = 0;
  is_inited_ = false;
}

int ObMicroBlockIndexReader::init(const ObFullMacroBlockMeta& meta, const char* index_buf)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMicroBlockIndexReader is inited twice", K(ret));
  } else if (!meta.is_valid() || OB_ISNULL(index_buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(meta), KP(index_buf));
  } else {
    ret = init(index_buf,
        meta.schema_->column_type_array_,
        meta.meta_->rowkey_column_number_,
        meta.meta_->micro_block_count_,
        meta.meta_->get_index_size(),
        meta.meta_->get_endkey_size(),
        meta.meta_->get_micro_block_mark_deletion_size(),
        meta.meta_->get_micro_block_delta_size(),
        meta.meta_->micro_block_data_offset_,
        (ObRowStoreType)meta.meta_->row_store_type_);
  }

  return ret;
}

int ObMicroBlockIndexReader::init(
    const ObSSTableMacroBlockHeader& header, const char* index_buf, const common::ObObjMeta* column_type_array)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMicroBlockIndexReader is inited twice", K(ret));
  } else if (!header.is_valid() || OB_ISNULL(index_buf) || OB_ISNULL(column_type_array)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(header), KP(index_buf), KP(column_type_array));
  } else {
    ret = init(index_buf,
        column_type_array,
        header.rowkey_column_count_,
        header.micro_block_count_,
        header.micro_block_index_size_,
        header.micro_block_endkey_size_,
        0, /*mark_deletion_buf_size*/
        0, /*delta_buf_size*/
        header.micro_block_data_offset_,
        (ObRowStoreType)(header.row_store_type_));
  }

  return ret;
}

int ObMicroBlockIndexReader::init_row_reader(const ObRowStoreType row_store_type)
{
  int ret = OB_SUCCESS;
  if (FLAT_ROW_STORE == row_store_type) {
    row_reader_ = &flat_row_reader_;
  } else if (SPARSE_ROW_STORE == row_store_type) {
    row_reader_ = &sparse_row_reader_;
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(row_store_type));
  }
  return ret;
}

int ObMicroBlockIndexReader::init(const char* index_buf, const common::ObObjMeta* column_type_array,
    const int32_t row_key_column_cnt, const int32_t micro_block_cnt, const int32_t index_buf_size,
    const int32_t endkey_buf_size, const int32_t mark_deletion_buf_size, const int32_t delta_buf_size,
    const int32_t data_base_offset, const ObRowStoreType row_store_type)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMicroBlockIndexReader is inited twice", K(ret));
  } else if (OB_ISNULL(index_buf) || OB_ISNULL(column_type_array) || row_key_column_cnt < 0 || micro_block_cnt < 0 ||
             index_buf_size < 0 || endkey_buf_size < 0 || mark_deletion_buf_size < 0 || delta_buf_size < 0 ||
             data_base_offset < 0 || MAX_ROW_STORE == row_store_type) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid argument",
        K(ret),
        KP(index_buf),
        KP(column_type_array),
        K(row_key_column_cnt),
        K(micro_block_cnt),
        K(index_buf_size),
        K(endkey_buf_size),
        K(mark_deletion_buf_size),
        K(delta_buf_size),
        K(data_base_offset),
        K(row_store_type));
  } else if ((0 != delta_buf_size && delta_buf_size / sizeof(int32_t) != micro_block_cnt) ||
             (0 != mark_deletion_buf_size && mark_deletion_buf_size / sizeof(uint8_t) != micro_block_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "mark deletion or delta count not equal to block count",
        K(ret),
        K(mark_deletion_buf_size),
        K(delta_buf_size),
        K(micro_block_cnt));
  } else if (OB_FAIL(init_row_reader(row_store_type))) {
    STORAGE_LOG(WARN, "init row reader failed", K(ret), K(row_store_type));
  } else {
    column_type_array_ = column_type_array;
    micro_indexes_ = reinterpret_cast<const ObMicroBlockIndex*>(index_buf);
    endkey_stream_ = index_buf + index_buf_size;
    mark_deletion_stream_ = (0 == mark_deletion_buf_size) ? nullptr : index_buf + index_buf_size + endkey_buf_size;
    delta_array_ = (0 == delta_buf_size) ? nullptr
                                         : reinterpret_cast<const int32_t*>(
                                               index_buf + index_buf_size + endkey_buf_size + mark_deletion_buf_size);
    block_count_ = micro_block_cnt;
    row_key_column_cnt_ = row_key_column_cnt;
    data_base_offset_ = data_base_offset;
    begin_ = Iterator(0);
    end_ = Iterator(block_count_);
    is_inited_ = true;
  }
  return ret;
}

int ObMicroBlockIndexReader::get_all_mem_micro_index(ObMicroBlockIndexMgr::MemMicroIndexItem* indexes)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexReader is not inited", K(ret));
  } else {
    for (int64_t i = 0; i <= block_count_; ++i) {
      indexes[i].data_offset_ = micro_indexes_[i].data_offset_;
    }
  }
  return ret;
}

int ObMicroBlockIndexReader::get_all_mark_deletions_flags(bool* mark_deletion_flags)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexReader is not inited", K(ret));
  } else if (OB_ISNULL(mark_deletion_stream_)) {
    // do nothing
  } else {
    for (int64_t i = 0; i < block_count_; ++i) {
      mark_deletion_flags[i] = static_cast<bool>(mark_deletion_stream_[i]);
    }
  }
  return ret;
}

int ObMicroBlockIndexReader::get_all_deltas(int32_t* delta_array)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "MicroBlockIndexReader is not inited", K(ret));
  } else if (OB_ISNULL(delta_array_)) {
    // old format block index, no delta array
  } else {
    MEMCPY(delta_array, delta_array_, sizeof(int32_t) * block_count_);
  }
  return ret;
}

int ObMicroBlockIndexReader::get_end_key(const uint64_t index, ObObj* objs)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  int64_t start_pos = 0;
  int64_t end_pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexReader is not inited", K(ret));
  } else if (OB_UNLIKELY(index >= block_count_ || nullptr == objs)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(index), K_(block_count), KP(objs));
  } else if (OB_ISNULL(row_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row reader is null", K(ret), K(index), K_(block_count), KP(objs));
  } else {
    row.cells_ = objs;
    row.count_ = 0;
    start_pos = micro_indexes_[index].endkey_offset_;
    end_pos = micro_indexes_[index + 1].endkey_offset_;
    if (OB_FAIL(row_reader_->read_compact_rowkey(
            column_type_array_, row_key_column_cnt_, endkey_stream_, end_pos, start_pos, row))) {
      STORAGE_LOG(WARN, "failed to read rowkey", K(ret), K(index), K(start_pos), K(end_pos));
    }
  }
  return ret;
}

int ObMicroBlockIndexReader::get_end_keys(
    const Iterator& begin, const Iterator& end, ObIAllocator& allocator, ObIArray<ObStoreRowkey>& end_keys)
{
  int ret = OB_SUCCESS;
  ObObj* objs = nullptr;
  for (Iterator it = begin; OB_SUCC(ret) && it <= end; ++it) {
    if (OB_ISNULL(objs = reinterpret_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * row_key_column_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate obj array", K(ret));
    } else if (OB_FAIL(get_end_key(*it, objs))) {
      STORAGE_LOG(WARN, "failed to get end key", K(it), K(ret));
    } else {
      ObStoreRowkey end_key(objs, row_key_column_cnt_);
      if (OB_FAIL(end_keys.push_back(end_key))) {
        STORAGE_LOG(WARN, "failed to push back end key", K(ret));
      }
    }
  }
  return ret;
}

int ObMicroBlockIndexReader::get_end_keys(
    const ObStoreRange& range, ObIAllocator& allocator, ObIArray<ObStoreRowkey>& end_keys)
{
  int ret = OB_SUCCESS;
  Iterator start_index;
  Iterator end_index;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexReader is not inited", K(ret));
  } else if (OB_FAIL(locate_start_index(range, start_index))) {
    STORAGE_LOG(WARN, "failed to locate start index", K(ret), K(range));
  } else if (OB_FAIL(locate_end_index(range, end_index))) {
    STORAGE_LOG(WARN, "failed to locate end index", K(ret), K(range));
  } else if (OB_FAIL(get_end_keys(start_index, end_index, allocator, end_keys))) {
    STORAGE_LOG(WARN, "failed to get end keys", K(ret), K(start_index), K(end_index), K_(block_count));
  }
  return ret;
}

int ObMicroBlockIndexReader::get_all_end_keys(ObIAllocator& allocator, ObIArray<ObStoreRowkey>& end_keys)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexReader is not inited", K(ret));
  } else if (OB_FAIL(get_end_keys(begin_, end_ - 1, allocator, end_keys))) {
    STORAGE_LOG(WARN, "failed to get end keys", K(ret));
  }
  return ret;
}

int ObMicroBlockIndexReader::get_micro_block_infos(
    const Iterator& begin, const Iterator& end, ObIArray<ObMicroBlockInfo>& micro_block_infos)
{
  int ret = OB_SUCCESS;
  ObMicroBlockInfo info;
  int32_t offset = -1;
  int32_t size = 0;
  int64_t index = -1;
  uint64_t idx = 0;
  bool mark_deletion_flag = false;

  for (Iterator it = begin; OB_SUCC(ret) && it <= end; ++it) {
    idx = *it;
    mark_deletion_flag = false;
    offset = micro_indexes_[idx].data_offset_ + data_base_offset_;
    size = micro_indexes_[idx + 1].data_offset_ - micro_indexes_[idx].data_offset_;
    index = idx;
    if (OB_NOT_NULL(mark_deletion_stream_)) {
      mark_deletion_flag = static_cast<bool>(mark_deletion_stream_[index]);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(info.set(offset, size, index, mark_deletion_flag))) {
        STORAGE_LOG(
            WARN, "failed to set micro block index info", K(ret), K(offset), K(size), K(index), K(mark_deletion_flag));
      } else if (OB_FAIL(micro_block_infos.push_back(info))) {
        STORAGE_LOG(WARN, "failed to push back micro block info", K(ret), K(it));
      }
    }
  }
  return ret;
}

int ObMicroBlockIndexReader::get_micro_block_infos(
    const ObStoreRange& range, ObIArray<ObMicroBlockInfo>& micro_block_infos)
{
  int ret = OB_SUCCESS;
  Iterator start_index;
  Iterator end_index;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMicroBlockIndexReader is not inited", K(ret));
  } else if (OB_FAIL(locate_start_index(range, start_index))) {
    STORAGE_LOG(WARN, "failed to locate start index", K(ret), K(range));
  } else if (OB_FAIL(locate_end_index(range, end_index))) {
    STORAGE_LOG(WARN, "failed to locate end index", K(ret), K(range));
  } else if (OB_FAIL(get_micro_block_infos(start_index, end_index, micro_block_infos))) {
    STORAGE_LOG(WARN, "failed to get micro block infos", K(ret), K(start_index), K(end_index));
  }
  return ret;
}

int ObMicroBlockIndexReader::locate_start_index(const ObStoreRange& range, Iterator& start_index)
{
  int ret = OB_SUCCESS;
  ObObj objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
  if (!range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid range", K(ret), K(range));
  } else if (range.get_start_key().is_min()) {
    start_index = begin();
  } else if (OB_FAIL(lower_bound(range.get_start_key(), objs, start_index))) {
    STORAGE_LOG(WARN, "failed to find lower bound", K(ret));
  } else if (end() == start_index) {
    ret = OB_BEYOND_THE_RANGE;
  } else if (!range.get_border_flag().inclusive_start()) {
    if (OB_FAIL(get_end_key(start_index.block_idx_, objs))) {
      STORAGE_LOG(WARN, "failed to get next rowkey", K(ret), K(start_index));
    } else {
      ObStoreRowkey rowkey(objs, row_key_column_cnt_);
      if (0 == rowkey.compare(range.get_start_key())) {
        ++start_index;
        if (end() == start_index) {
          ret = OB_BEYOND_THE_RANGE;
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockIndexReader::locate_end_index(const ObStoreRange& range, Iterator& end_index)
{
  int ret = OB_SUCCESS;
  ObObj objs[OB_MAX_ROWKEY_COLUMN_NUMBER];
  if (!range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid range", K(ret), K(range));
  } else if (range.get_end_key().is_max()) {
    end_index = end() - 1;
  } else if (OB_FAIL(lower_bound(range.get_end_key(), objs, end_index))) {
    STORAGE_LOG(WARN, "failed to find lower bound", K(ret));
  } else if (end() == end_index) {
    --end_index;
  }
  return ret;
}

int ObMicroBlockIndexReader::lower_bound(const ObStoreRowkey& key, ObObj* objs, Iterator& index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!key.is_valid() || NULL == objs)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(key), K(objs));
  } else {
    ObBlockIndexCompare compare(*this, objs, row_key_column_cnt_);
    index = std::lower_bound(begin(), end(), key, compare);
    if (OB_FAIL(compare.get_ret())) {
      STORAGE_LOG(WARN, "failed to find lower bound", K(ret), K(key));
    }
  }
  if (OB_FAIL(ret)) {
    index = Iterator::invalid_iterator();
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
