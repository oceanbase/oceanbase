/**
 *
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
#include "share/schema/ob_column_schema.h"
#include "ob_index_block_row_scanner.h"
#include "ob_index_block_row_struct.h"
#include "storage/access/ob_rows_info.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase
{
using namespace storage;
namespace blocksstable
{

int ObIndexBlockDataHeader::get_index_data(
    const int64_t row_idx, const char *&index_ptr, int64_t &index_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || row_idx >= row_cnt_ || row_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid row count", K(ret), K(row_idx), K_(row_cnt), KPC(this));
  } else {
    const int64_t obj_idx = (row_idx + 1) * col_cnt_ - 1;
    const ObStorageDatum &datum = datum_array_[obj_idx];
    ObString index_data_buf = datum.get_string();
    if (OB_UNLIKELY(index_data_buf.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null index data buf", K(ret), K(datum), K(row_idx));
    } else {
      index_ptr = index_data_buf.ptr();
      index_len = index_data_buf.length();
    }
  }
  return ret;
}

int ObIndexBlockDataHeader::deep_copy_transformed_index_block(
    const ObIndexBlockDataHeader &header,
    const int64_t buf_size,
    char *buf,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!header.is_valid() || buf_size < 0 || pos >= buf_size || pos + header.data_buf_size_ > buf_size)
      || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for copy transformed index block", K(ret), KP(buf),
        K(header), K(buf_size), K(pos));
  } else {
    char *data_buf = buf + pos;
    const int64_t datum_cnt = header.row_cnt_ * header.col_cnt_;
    ObDatumRowkey *rowkey_arr = new (buf + pos) ObDatumRowkey[header.row_cnt_];
    pos += sizeof(ObDatumRowkey) * header.row_cnt_;
    ObStorageDatum *datum_arr = new (buf + pos) ObStorageDatum[datum_cnt];
    pos += sizeof(ObStorageDatum) * datum_cnt;
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < header.row_cnt_; ++row_idx) {
      if (OB_FAIL(rowkey_arr[row_idx].assign(datum_arr + row_idx * header.col_cnt_, header.col_cnt_ - 1))) {
        LOG_WARN("Fail to assign datum array to rowkey", K(ret), K(header));
      }
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < header.col_cnt_; ++col_idx) {
        const int64_t datum_idx = row_idx * header.col_cnt_ + col_idx;
        if (OB_FAIL(datum_arr[datum_idx].deep_copy(header.datum_array_[datum_idx], buf, buf_size, pos))) {
          LOG_WARN("Fail to deep copy datum", K(ret), K(datum_idx), K(header), K(header.datum_array_[datum_idx]));
        }
      }
    }

    if (OB_SUCC(ret)) {
      row_cnt_ = header.row_cnt_;
      col_cnt_ = header.col_cnt_;
      rowkey_array_ = rowkey_arr;
      datum_array_ = datum_arr;
      data_buf_ = data_buf;
      data_buf_size_ = header.data_buf_size_;
    }
  }
  return ret;
}

ObIndexBlockDataTransformer::ObIndexBlockDataTransformer()
  : allocator_(SET_USE_500(lib::ObMemAttr(OB_SERVER_TENANT_ID, "IdxBlkDataTrans"))), micro_reader_helper_() {}

ObIndexBlockDataTransformer::~ObIndexBlockDataTransformer()
{
}

// Transform block data to look-up format and store in transform buffer
int ObIndexBlockDataTransformer::transform(
    const ObMicroBlockData &block_data,
    ObMicroBlockData &transformed_data,
    ObIAllocator &allocator,
    char *&allocated_buf)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  char *block_buf = nullptr; // transformed block buf
  int64_t pos = 0;
  ObIMicroBlockReader *micro_reader = nullptr;
  ObMicroBlockHeader *new_micro_header = nullptr;
  const ObMicroBlockHeader *micro_block_header =
      reinterpret_cast<const ObMicroBlockHeader *>(block_data.get_buf());
  const int64_t col_cnt = micro_block_header->column_count_;
  const int64_t row_cnt = micro_block_header->row_count_;
  int64_t mem_limit = 0;
  if (OB_UNLIKELY(!block_data.is_valid() || !micro_block_header->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(block_data), KPC(micro_block_header));
  } else if (OB_FAIL(get_reader(block_data.get_store_type(), micro_reader))) {
    LOG_WARN("Fail to set micro block reader", K(ret));
  } else if (OB_FAIL(micro_reader->init(block_data, nullptr))) {
    LOG_WARN("Fail to init micro block reader", K(ret), K(block_data));
  } else if (OB_FAIL(row.init(allocator, col_cnt))) {
    LOG_WARN("Failed to init datum row", K(ret), K(col_cnt));
  } else if (OB_FAIL(get_transformed_upper_mem_size(block_data.get_buf(), mem_limit))) {
    LOG_WARN("Failed to get upper bound of transformed block size", K(ret), K(block_data));
  } else if (OB_ISNULL(block_buf = static_cast<char *>(allocator.alloc(mem_limit)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for transformed block buf", K(ret), K(mem_limit));
  } else if (OB_FAIL(micro_block_header->deep_copy(block_buf, mem_limit, pos, new_micro_header))) {
    LOG_WARN("Failed to serialize micro block header to transformed buf",
        K(ret), KPC(micro_block_header), K(pos));
  } else if (OB_UNLIKELY(!new_micro_header->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid copied micro block header", K(ret), KPC(new_micro_header));
  } else {
    const int64_t micro_header_size = pos;
    const int64_t datum_cnt = row_cnt * col_cnt;
    ObIndexBlockDataHeader *idx_header = new (block_buf + pos) ObIndexBlockDataHeader();
    pos += sizeof(ObIndexBlockDataHeader);
    ObDatumRowkey *rowkey_arr = new (block_buf + pos) ObDatumRowkey[row_cnt];
    pos += sizeof(ObDatumRowkey) * row_cnt;
    ObStorageDatum *datum_arr = new (block_buf + pos) ObStorageDatum[datum_cnt];
    pos += sizeof(ObStorageDatum) * datum_cnt;

    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_cnt; ++row_idx) {
      row.reuse();
      if (OB_FAIL(micro_reader->get_row(row_idx, row))) {
        LOG_WARN("Fail to get row", K(ret), K(row_idx));
      } else {
        for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_cnt; ++col_idx) {
          const int64_t datum_idx = row_idx * col_cnt + col_idx;
          if (OB_UNLIKELY(datum_idx >= datum_cnt)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected datum size overflow", K(ret), K(datum_idx), K(datum_cnt));
          } else if (OB_FAIL(datum_arr[datum_idx].deep_copy(
              row.storage_datums_[col_idx], block_buf, mem_limit, pos))) {
            LOG_WARN("Fail to deep copy storage datum to buf", K(ret), K(row_idx), K(col_idx), K(col_cnt));
          }
        }
        if (FAILEDx(rowkey_arr[row_idx].assign(datum_arr + row_idx * col_cnt, col_cnt - 1))) {
          LOG_WARN("Fail to assign datum array to rowkey", K(ret), K(row), K(row_idx));
        }
      }
    }

    if (OB_SUCC(ret)) {
      idx_header->row_cnt_ = row_cnt;
      idx_header->col_cnt_ = col_cnt;
      idx_header->rowkey_array_ = rowkey_arr;
      idx_header->datum_array_ = datum_arr;
      idx_header->data_buf_size_ = pos - micro_header_size;
      transformed_data.buf_ = block_buf;
      transformed_data.size_ = micro_header_size;
      transformed_data.extra_buf_ = block_buf + micro_header_size;
      transformed_data.extra_size_ = idx_header->data_buf_size_;
      transformed_data.type_ = ObMicroBlockData::INDEX_BLOCK;
      allocated_buf = block_buf;
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to transform index block to in_memory format", K(ret),
        KPC(micro_block_header), KPC(new_micro_header), K(block_data));
    if (nullptr != block_buf) {
      allocator.free(block_buf);
    }
  }
  return ret;
}

int ObIndexBlockDataTransformer::fix_micro_header_and_transform(
    const ObMicroBlockData &raw_data,
    ObMicroBlockData &transformed_data,
    ObIAllocator &allocator,
    char *&allocated_buf)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator; // tmp allocator to fix micro header
  ObDatumRow row;
  ObIMicroBlockReader *micro_reader = nullptr;
  const ObMicroBlockHeader *micro_block_header =
      reinterpret_cast<const ObMicroBlockHeader *>(raw_data.get_buf());
  if (OB_UNLIKELY(!raw_data.is_valid() || !micro_block_header->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(raw_data), KPC(micro_block_header));
  } else if (OB_FAIL(get_reader(raw_data.get_store_type(), micro_reader))) {
    LOG_WARN("Fail to set micro block reader", K(ret));
  } else if (OB_FAIL(micro_reader->init(raw_data, nullptr))) {
    LOG_WARN("Fail to init micro block reader", K(ret), K(raw_data));
  } else if (OB_FAIL(row.init(allocator, micro_block_header->column_count_))) {
    LOG_WARN("Failed to init datum row", K(ret), KPC(micro_block_header));
  } else {
    int64_t raw_data_size = 0;
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < micro_block_header->row_count_; ++row_idx) {
      row.reuse();
      if (OB_FAIL(micro_reader->get_row(row_idx, row))) {
        LOG_WARN("Fail to get row", K(ret), K(row_idx));
      }
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < micro_block_header->column_count_; ++col_idx) {
        if (row.storage_datums_[col_idx].is_null()) {
          raw_data_size += sizeof(int64_t);
        } else {
          raw_data_size += row.storage_datums_[col_idx].len_;
        }
      }
    }
    ObMicroBlockHeader new_micro_header;
    char *new_data_buf = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(new_data_buf = static_cast<char *>(tmp_allocator.alloc(raw_data.get_buf_size())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for new micro block buf", K(ret), K(raw_data));
    } else {
      MEMCPY(new_data_buf, raw_data.get_buf(), raw_data.get_buf_size());
      int64_t copy_pos = 0;
      ObMicroBlockHeader *copied_header = nullptr;
      if (OB_FAIL(raw_data.get_micro_header()->deep_copy(new_data_buf, raw_data.get_buf_size(), copy_pos, copied_header))) {
        LOG_WARN("Failed to deep copy micro header", K(ret), K(raw_data), KPC(raw_data.get_micro_header()));
      } else {
        copied_header->data_length_ = raw_data.get_buf_size() - copy_pos;
        copied_header->data_zlength_ = copied_header->data_length_;
        copied_header->original_length_ = raw_data_size;
        ObMicroBlockData copied_micro_data(new_data_buf, raw_data.get_buf_size());
        if (OB_FAIL(transform(copied_micro_data, transformed_data, allocator, allocated_buf))) {
          LOG_WARN("Failed to transform with copied micro data", K(ret),
              KPC(micro_block_header), K(new_micro_header), KPC(copied_header));
        }
      }
    }
  }
  return ret;
}

int ObIndexBlockDataTransformer::get_transformed_upper_mem_size(
    const char *raw_block_data,
    int64_t &mem_limit)
{
  int ret = OB_SUCCESS;
  mem_limit = 0;
  const ObMicroBlockHeader *micro_header =
      reinterpret_cast<const ObMicroBlockHeader *>(raw_block_data);
  if (OB_ISNULL(raw_block_data) || OB_UNLIKELY(!micro_header->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(raw_block_data), KPC(micro_header));
  } else {
    mem_limit += micro_header->get_serialize_size();
    mem_limit += sizeof(ObIndexBlockDataHeader);
    mem_limit += micro_header->row_count_ * sizeof(ObDatumRowkey);
    mem_limit += micro_header->row_count_ * (sizeof(ObStorageDatum) * micro_header->column_count_);
    mem_limit += micro_header->original_length_;
  }
  return ret;
}

int ObIndexBlockDataTransformer::get_reader(
    const ObRowStoreType store_type,
    ObIMicroBlockReader *&micro_reader)
{
  int ret = OB_SUCCESS;
  if (!micro_reader_helper_.is_inited() && OB_FAIL(micro_reader_helper_.init(allocator_))) {
    LOG_WARN("Fail to init micro reader helper", K(ret));
  } else if (OB_FAIL(micro_reader_helper_.get_reader(store_type, micro_reader))) {
    LOG_WARN("Fail to get micro reader", K(ret), K(store_type));
  }
  return ret;
}

ObIndexBlockRowScanner::ObIndexBlockRowScanner()
  : query_range_(nullptr), agg_projector_(nullptr), agg_column_schema_(nullptr),
    idx_data_header_(nullptr), macro_id_(), allocator_(nullptr),
    micro_reader_helper_(), micro_reader_(nullptr),
    block_meta_tree_(nullptr), datum_row_(nullptr), endkey_(),
    idx_row_parser_(), datum_utils_(nullptr),
    current_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    start_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    end_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    step_(1), range_idx_(0), nested_offset_(0), rowkey_begin_idx_(0), rowkey_end_idx_(0),
    index_format_(IndexFormat::INVALID), parent_row_range_(), is_get_(false), is_reverse_scan_(false),
    is_left_border_(false), is_right_border_(false), is_inited_(false),
    is_normal_cg_(false), filter_constant_type_(sql::ObBoolMaskType::PROBABILISTIC)
{}

ObIndexBlockRowScanner::~ObIndexBlockRowScanner() {}

void ObIndexBlockRowScanner::reuse()
{
  query_range_ = nullptr;
  idx_data_header_ = nullptr;
  current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  start_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  end_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  block_meta_tree_ = nullptr;
  index_format_ = IndexFormat::INVALID;
  is_left_border_ = false;
  is_right_border_ = false;
  parent_row_range_.reset();
  filter_constant_type_ = sql::ObBoolMaskType::PROBABILISTIC;
}

void ObIndexBlockRowScanner::reset()
{
  query_range_ = nullptr;
  idx_data_header_ = nullptr;
  micro_reader_helper_.reset();
  parent_row_range_.reset();
  micro_reader_ = nullptr;
  block_meta_tree_ = nullptr;
  if (nullptr != datum_row_) {
    datum_row_->~ObDatumRow();
    if (nullptr != allocator_) {
      allocator_->free(datum_row_);
    }
    datum_row_ = nullptr;
  }
  datum_utils_ = nullptr;
  current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  start_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  end_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  step_ = 1;
  range_idx_ = 0;
  nested_offset_ = 0;
  rowkey_begin_idx_ = 0;
  rowkey_end_idx_ = 0;
  index_format_ = IndexFormat::INVALID;
  is_get_ = false;
  is_reverse_scan_ = false;
  is_left_border_ = false;
  is_right_border_ = false;
  is_inited_ = false;
  is_normal_cg_ = false;
  filter_constant_type_ = sql::ObBoolMaskType::PROBABILISTIC;
}

int ObIndexBlockRowScanner::init(
    const ObIArray<int32_t> &agg_projector,
    const ObIArray<share::schema::ObColumnSchemaV2> &agg_column_schema,
    const ObStorageDatumUtils &datum_utils,
    ObIAllocator &allocator,
    const common::ObQueryFlag &query_flag,
    const int64_t nested_offset,
    const bool is_normal_cg)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Already inited", K(ret));
  } else if (OB_UNLIKELY(agg_projector.count() != agg_column_schema.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Agg meta count not same", K(ret), K(agg_projector), K(agg_column_schema));
  } else if (OB_FAIL(micro_reader_helper_.init(allocator))) {
    LOG_WARN("Fail to init micro reader helper", K(ret));
  } else {
    agg_projector_ = &agg_projector;
    agg_column_schema_ = &agg_column_schema;
    allocator_ = &allocator;
    is_reverse_scan_ = query_flag.is_reverse_scan();
    step_ = is_reverse_scan_ ? -1 : 1;
    datum_utils_ = &datum_utils;
    nested_offset_ = nested_offset;
    is_normal_cg_ = is_normal_cg;
    is_inited_ = true;
  }
  return ret;
}

int ObIndexBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &idx_block_data,
    const ObDatumRowkey &rowkey,
    const int64_t range_idx,
    const ObMicroIndexInfo *idx_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !idx_block_data.is_valid() || !rowkey.is_valid()
      || !idx_block_data.is_index_block() || (is_normal_cg_ && nullptr == idx_info))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to open an index micro block", K(ret),
        K(macro_id), K(idx_block_data), K(rowkey), K_(is_normal_cg), KP(idx_info));
  } else if (OB_FAIL(init_by_micro_data(idx_block_data))) {
    LOG_WARN("Fail to init scanner by micro data", K(ret), K(idx_block_data));
  } else if (is_normal_cg_ && !idx_info->is_root() && idx_info->is_macro_node()) {
    // Rowkey offset in macro node is local
    ObStorageDatum offset;
    ObDatumRowkey offset_rowkey;
    offset.set_int(rowkey.datums_[0].get_int() - idx_info->get_row_range().start_row_id_);
    offset_rowkey.assign(&offset, 1);
    if (OB_FAIL(locate_key(offset_rowkey))) {
      LOG_WARN("Fail to locate rowkey", K(ret), K(idx_block_data), K(offset_rowkey));
    }
  } else if (OB_FAIL(locate_key(rowkey))) {
    LOG_WARN("Fail to locate rowkey", K(ret), K(idx_block_data), K(rowkey));
  }
  if (OB_SUCC(ret)) {
    macro_id_ = macro_id;
    range_idx_ = range_idx;
    rowkey_ = &rowkey;
    is_get_ = true;
    if (nullptr != idx_info) {
      parent_row_range_ = idx_info->get_row_range();
    } else {
      parent_row_range_.reset();
    }
  }
  return ret;
}

int ObIndexBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &idx_block_data,
    const ObRowsInfo *rows_info,
    const int64_t rowkey_begin_idx,
    const int64_t rowkey_end_idx)
{
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !idx_block_data.is_valid() || nullptr == rows_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to open an index micro block", K(ret), K(macro_id), K(idx_block_data),
              KP(rows_info));
  } else if (OB_FAIL(init_by_micro_data(idx_block_data))) {
    LOG_WARN("Fail to init scanner by micro data", K(ret), K(idx_block_data));
  } else if (IndexFormat::TRANSFORMED == index_format_) {
    row_count = idx_data_header_->row_cnt_;
  } else if (IndexFormat::BLOCK_TREE == index_format_) {
    row_count = block_meta_tree_->get_rowkey_count();
  } else if (IndexFormat::RAW_DATA == index_format_) {
    if (OB_FAIL(micro_reader_->get_row_count(row_count))) {
      LOG_WARN("Failed to get row count", K(ret), K(idx_block_data));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected index format", K_(index_format));
  }

  if (OB_SUCC(ret)) {
    start_ = 0;
    end_ = row_count - 1;
    macro_id_ = macro_id;
    rows_info_ = rows_info;
    rowkey_begin_idx_ = rowkey_begin_idx;
    rowkey_end_idx_ = rowkey_end_idx;
    current_ = 0;
    is_get_ = false;
  }
  return ret;
}

int ObIndexBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &idx_block_data,
    const ObDatumRange &range,
    const int64_t range_idx,
    const bool is_left_border,
    const bool is_right_border,
    const ObMicroIndexInfo *idx_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !idx_block_data.is_valid() || !range.is_valid()
      || !idx_block_data.is_index_block() || (is_normal_cg_ && nullptr == idx_info))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to open an index micro block", K(ret), K(idx_block_data), K(range), K_(is_normal_cg), KP(idx_info));
  } else if (OB_FAIL(init_by_micro_data(idx_block_data))) {
    LOG_WARN("Fail to init scanner by micro data", K(ret), K(idx_block_data));
  } else if (OB_FAIL(locate_range(range, is_left_border, is_right_border))) {
    if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
      LOG_WARN("Fail to locate range", K(ret), K(range), K(is_left_border), K(is_right_border));
    }
  } else {
    macro_id_ = macro_id;
    is_left_border_ = is_left_border;
    is_right_border_ = is_right_border;
    range_idx_ = range_idx;
    is_get_ = false;
    if (nullptr != idx_info) {
      parent_row_range_ = idx_info->get_row_range();
      filter_constant_type_ = idx_info->get_filter_constant_type();
    } else {
      parent_row_range_.reset();
    }
  }
  return ret;
}

int ObIndexBlockRowScanner::get_next(
    ObMicroIndexInfo &idx_block_row,
    const bool is_multi_check)
{
  int ret = OB_SUCCESS;
  idx_block_row.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (end_of_block()) {
    ret = OB_ITER_END;
  } else if (is_multi_check && OB_FAIL(skip_to_next_valid_position(idx_block_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("Failed to skip to next valid position", K(ret));
    } else {
      current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    }
  } else if (OB_FAIL(get_next_idx_row(idx_block_row))) {
    LOG_WARN("Failed to get next idx row", K(ret), K(is_multi_check));
  }
  return ret;
}

int ObIndexBlockRowScanner::get_cur_row_id_range(ObCSRange &cs_range)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  const ObDatumRowkey *endkey = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K_(is_inited));
  } else if (end_of_block()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected end of index block scanner", KPC(this));
  } else if (OB_FAIL(read_curr_idx_row(idx_row_header, endkey))) {
    LOG_WARN("Fail to read currend index row", K(ret), K(index_format_), K_(current));
  } else if (OB_ISNULL(idx_row_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index block row header", K(ret));
  } else {
    cs_range.start_row_id_ = idx_row_parser_.get_row_offset() - idx_row_header->get_row_count() + 1;
    cs_range.end_row_id_ = idx_row_parser_.get_row_offset();
    if (idx_row_header->is_data_block()) {
      cs_range.start_row_id_ += parent_row_range_.start_row_id_;
      cs_range.end_row_id_ += parent_row_range_.start_row_id_;
    }
    LOG_DEBUG("ObIndexBlockRowScanner::get_cur_row_id_range",
               K(cs_range), K_(parent_row_range), K_(current), K_(start), K_(end));
  }
  return ret;
}

bool ObIndexBlockRowScanner::end_of_block() const
{
  return current_ < start_
      || current_ > end_
      || current_ == ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
}

int ObIndexBlockRowScanner::get_index_row_count(int64_t &index_row_count) const
{
  int ret = OB_SUCCESS;
  index_row_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (start_ < 0 || end_ < 0) {
    index_row_count = 0;
  } else {
    index_row_count = end_ - start_ + 1;
  }
  return ret;
}

int ObIndexBlockRowScanner::check_blockscan(
    const ObDatumRowkey &rowkey,
    bool &can_blockscan)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (IndexFormat::BLOCK_TREE == index_format_) {
    can_blockscan = false;
  } else if (is_reverse_scan_) {
    if (rowkey.is_min_rowkey()) {
      can_blockscan = true;
    } else {
      // TODO(yuanzhe) opt this
      can_blockscan = false;
    }
  } else if (rowkey.is_max_rowkey()) {
    can_blockscan = true;
  } else {
    int cmp_ret = 0;
    if (IndexFormat::RAW_DATA == index_format_) {
      ObDatumRowkey last_endkey;
      ObDatumRow tmp_datum_row; // Normally will use local datum buf, won't allocate memory
      const int64_t request_cnt = datum_utils_->get_rowkey_count() + 1;
      if (OB_FAIL(tmp_datum_row.init(request_cnt))) {
        LOG_WARN("Fail to init tmp_datum_row", K(ret));
      } else if (OB_FAIL(micro_reader_->get_row(end_, tmp_datum_row))) {
        LOG_WARN("Fail to get last row of micro block", K(ret), K_(end));
      } else if (OB_FAIL(last_endkey.assign(tmp_datum_row.storage_datums_, datum_utils_->get_rowkey_count()))) {
        LOG_WARN("Fail to assign storage datum to endkey", K(ret), K(tmp_datum_row));
      } else if (OB_FAIL(last_endkey.compare(rowkey, *datum_utils_, cmp_ret, false))) {
        LOG_WARN("Fail to compare rowkey", K(ret), K(last_endkey), K(rowkey));
      }
    } else if (OB_FAIL((idx_data_header_->rowkey_array_ + end_)->compare(rowkey, *datum_utils_, cmp_ret, false))) {
      LOG_WARN("Fail to compare rowkey", K(ret), K(rowkey));
    }

    if (OB_FAIL(ret)) {
    } else if (cmp_ret < 0) {
      can_blockscan = true;
    } else {
      can_blockscan = false;
    }
  }
  return ret;
}

int ObIndexBlockRowScanner::init_by_micro_data(const ObMicroBlockData &idx_block_data)
{
  int ret = OB_SUCCESS;
  if (ObMicroBlockData::INDEX_BLOCK == idx_block_data.type_) {
    if (nullptr == idx_block_data.get_extra_buf()) {
      if (OB_FAIL(micro_reader_helper_.get_reader(idx_block_data.get_store_type(), micro_reader_))) {
        LOG_WARN("Fail to get micro block reader", K(ret),
            K(idx_block_data), K(idx_block_data.get_store_type()));
      } else if (OB_FAIL(micro_reader_->init(idx_block_data, datum_utils_))) {
        LOG_WARN("Fail to init micro reader", K(ret), K(idx_block_data));
      } else if (OB_FAIL(init_datum_row())) {
        LOG_WARN("Fail to init datum row", K(ret));
      } else {
        index_format_ = IndexFormat::RAW_DATA;
        idx_data_header_ = nullptr;
      }
    } else {
      idx_data_header_ = reinterpret_cast<const ObIndexBlockDataHeader *>(idx_block_data.get_extra_buf());
      if (OB_UNLIKELY(!idx_data_header_->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Invalid index block data header", K(ret), KPC(idx_data_header_));
      } else {
        index_format_ = IndexFormat::TRANSFORMED;
      }
    }
  } else if (ObMicroBlockData::DDL_BLOCK_TREE == idx_block_data.type_) {
    block_meta_tree_ = reinterpret_cast<ObBlockMetaTree *>(const_cast<char *>(idx_block_data.buf_));
    index_format_ = IndexFormat::BLOCK_TREE;
  }
  return ret;
}

int ObIndexBlockRowScanner::locate_key(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  int64_t begin_idx = -1;
  int64_t end_idx = -1;
  if (IndexFormat::RAW_DATA == index_format_) {
    ObDatumRange range;
    range.set_start_key(rowkey);
    range.end_key_.set_max_rowkey();
    range.set_left_closed();
    range.set_right_open();
    if (OB_FAIL(micro_reader_->locate_range(range, true, false, begin_idx, end_idx, true))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("Fail to locate range in micro data", K(ret));
      } else {
        current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
        ret = OB_SUCCESS; // return OB_ITER_END on get_next() for get
      }
    }
    LOG_TRACE("Binary search rowkey with micro reader", K(ret), K(range), K(begin_idx), K(rowkey));
  } else if (IndexFormat::TRANSFORMED == index_format_) {
    ObDatumComparor<ObDatumRowkey> cmp(*datum_utils_, ret);
    const ObDatumRowkey *first = idx_data_header_->rowkey_array_;
    const ObDatumRowkey *last = idx_data_header_->rowkey_array_ + idx_data_header_->row_cnt_;
    const ObDatumRowkey *found = std::lower_bound(first, last, rowkey, cmp);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get rowkey lower_bound", K(ret), K(rowkey), KPC(idx_data_header_));
    } else if (found == last) {
      current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    } else {
      begin_idx = found - first;
    }
    LOG_TRACE("Binary search rowkey in transformed block", K(ret), KP(found), KPC(first), KP(last),
        K(current_), K(rowkey), KPC(idx_data_header_));
  } else if (IndexFormat::BLOCK_TREE == index_format_) {
    ObDatumRange range;
    range.set_start_key(rowkey);
    range.set_end_key(rowkey);
    range.set_left_closed();
    range.set_right_closed();
    if (OB_ISNULL(block_meta_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block meta tree is null", K(ret));
    } else if (OB_FAIL(block_meta_tree_->locate_range(range,
                                                      *datum_utils_,
                                                      true,// is_left_border
                                                      true,// is_right_border
                                                      begin_idx,
                                                      end_idx))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("locate rowkey failed", K(ret), K(range));
      } else {
        current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
        ret = OB_SUCCESS; // return OB_ITER_END on get_next() for get
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported index format", K(ret), K(index_format_));
  }

  if (OB_SUCC(ret)) {
    current_ = begin_idx;
    start_ = begin_idx;
    end_ = begin_idx;
  }
  return ret;
}

int ObIndexBlockRowScanner::locate_range(
    const ObDatumRange &range,
    const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  int64_t begin_idx = -1;
  int64_t end_idx = -1;
  current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  if (IndexFormat::TRANSFORMED == index_format_) {
    bool is_begin_equal = false;
    ObDatumComparor<ObDatumRowkey> lower_bound_cmp(*datum_utils_, ret);
    ObDatumComparor<ObDatumRowkey> upper_bound_cmp(*datum_utils_, ret, false, false);
    const ObDatumRowkey *first = idx_data_header_->rowkey_array_;
    const ObDatumRowkey *last = idx_data_header_->rowkey_array_ + idx_data_header_->row_cnt_;
    if (!is_left_border || range.get_start_key().is_min_rowkey()) {
      begin_idx = 0;
    } else {
      const ObDatumRowkey *start_found = std::lower_bound(first, last, range.get_start_key(), lower_bound_cmp);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get rowkey lower_bound", K(ret), K(range), KPC(idx_data_header_));
      } else if (start_found == last) {
        ret = OB_BEYOND_THE_RANGE;
      } else if (!range.get_border_flag().inclusive_start()) {
        bool is_equal = false;
        if (OB_FAIL(start_found->equal(range.get_start_key(), *datum_utils_, is_equal))) {
          STORAGE_LOG(WARN, "Failed to check datum rowkey equal", K(ret), K(range), KPC(start_found));
        } else if (is_equal) {
          ++start_found;
          if (start_found == last) {
            ret = OB_BEYOND_THE_RANGE;
          }
        }
      }
      if (OB_SUCC(ret)) {
        begin_idx = start_found - first;
      }
    }
    LOG_TRACE("Locate range start key in index block by range", K(ret),
        K(range), K(begin_idx), KPC(first), K(idx_data_header_->row_cnt_));

    if (OB_FAIL(ret)) {
    } else if (!is_right_border || range.get_end_key().is_max_rowkey()) {
      end_idx = idx_data_header_->row_cnt_ - 1;
    } else {
      const ObDatumRowkey *end_found = nullptr;
      // TODO remove is_normal_cg_, use flag in header
      // no need to use upper_bound for column store
      if (!is_normal_cg_ && range.get_border_flag().inclusive_end()) {
        end_found = std::upper_bound(first, last, range.get_end_key(), upper_bound_cmp);
      } else {
        end_found = std::lower_bound(first, last, range.get_end_key(), lower_bound_cmp);
      }

      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get rowkey lower_bound", K(ret), K(range), KPC(idx_data_header_));
      } else if (end_found == last) {
        --end_found;
      }
      if (OB_SUCC(ret)) {
        end_idx = end_found - first;
        if (OB_UNLIKELY(end_idx < begin_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected end of range less than start of range", K(ret), K(range), K_(end),
              K_(start), K(is_begin_equal), KPC(idx_data_header_));
        }
      }
    }
    LOG_TRACE("Locate range in index block by range", K(ret), K(range), K(begin_idx), K(end_idx),
      K(is_left_border), K(is_right_border), K_(current), KPC(idx_data_header_));
  } else if (IndexFormat::RAW_DATA == index_format_) {
    if (OB_FAIL(micro_reader_->locate_range(
            range, is_left_border, is_right_border, begin_idx, end_idx, true))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("Fail to locate range with micro reader", K(ret));
      }
    } else {
      LOG_TRACE("Binary search range with micro reader", K(ret), K(range), K(begin_idx), K(end_idx));
    }
  } else if (IndexFormat::BLOCK_TREE == index_format_) {
    if (OB_ISNULL(block_meta_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block meta tree is null", K(ret));
    } else if (OB_FAIL(block_meta_tree_->locate_range(range,
                                                      *datum_utils_,
                                                      is_left_border,
                                                      is_right_border,
                                                      begin_idx,
                                                      end_idx))) {
      if (OB_UNLIKELY(OB_BEYOND_THE_RANGE != ret)) {
        LOG_WARN("locate rowkey failed", K(ret), K(range));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported index format", K(ret), K(index_format_));
  }

  if (OB_SUCC(ret)) {
    start_ = begin_idx;
    end_ = end_idx;
    current_ = is_reverse_scan_ ? end_ : start_;
    range_ = &range;
    LOG_TRACE("Locate range in index block by range", K(ret), K(range), K(begin_idx), K(end_idx),
      K(is_left_border), K(is_right_border), K_(current), KPC(idx_data_header_), KP(this));
  }
  return ret;
}

int ObIndexBlockRowScanner::advance_to_border(
    const ObDatumRowkey &rowkey,
    const int32_t range_idx,
    ObCSRange &cs_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index_format_ != IndexFormat::TRANSFORMED)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected error", K(ret), K(index_format_));
  } else if (OB_UNLIKELY(end_of_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected error", K(ret), K(end_of_block()));
  } else if (range_idx == range_idx_) {
    const int64_t limit_idx = is_reverse_scan_ ? start_ : end_;
    if(OB_FAIL(advance_to_border(rowkey, limit_idx, cs_range))) {
      LOG_WARN("Failed to advance to border", K(range_idx));
    }
  }
  return ret;
}

int ObIndexBlockRowScanner::get_idx_row_header_in_target_idx(
    const ObIndexBlockRowHeader *&idx_row_header,
    const int64_t idx)
{
  int ret = OB_SUCCESS;
  idx_row_parser_.reset();
  const char *idx_data_buf = nullptr;
  int64_t idx_data_len = 0;
  if (OB_FAIL(idx_data_header_->get_index_data(idx, idx_data_buf, idx_data_len))) {
    LOG_WARN("Fail to get index data", K(idx), K_(start), K_(end), K_(current), KPC_(idx_data_header));
  } else if (OB_FAIL(idx_row_parser_.init(idx_data_buf, idx_data_len))) {
    LOG_WARN("Fail to parse index block row", K(idx), KPC(idx_data_header_));
  } else if (OB_FAIL(idx_row_parser_.get_header(idx_row_header))) {
    LOG_WARN("Fail to get index block row header", KPC(idx_row_header));
  }
  return ret;
}

int ObIndexBlockRowScanner::advance_to_border(
    const ObDatumRowkey &rowkey,
    const int64_t limit_idx,
    ObCSRange &cs_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(end_of_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected error", K(ret), K(end_of_block()));
  } else {
    const bool is_range_end = is_reverse_scan_ ? is_left_border_ : is_right_border_;
    ObDatumComparor<ObDatumRowkey> lower_bound_cmp(*datum_utils_, ret, false, true, false);
    const ObDatumRowkey *first = nullptr;
    const ObDatumRowkey *last = nullptr;
    if (!is_reverse_scan_) {
      first = idx_data_header_->rowkey_array_ + current_;
      last = idx_data_header_->rowkey_array_ + limit_idx + 1;
    } else {
      first = idx_data_header_->rowkey_array_ + limit_idx;
      last = idx_data_header_->rowkey_array_ + current_ + 1;
    }
    const ObDatumRowkey *start_found = std::lower_bound(first, last, rowkey, lower_bound_cmp);
    if (OB_FAIL(ret)) {
      LOG_WARN("Failed to get rowkey lower bound", K(ret), K(rowkey), KPC(idx_data_header_));
    } else if (!is_reverse_scan_) {
      // found_pos is safe to skip(end_key < border_rowkey).
      int64_t found_pos = start_found - idx_data_header_->rowkey_array_ - 1;
      if (is_range_end && found_pos == limit_idx) {
        // if is_range_end is true, we cannot skip all rowids because only subset of rowids statisy query range.
        found_pos--;
      }
      LOG_DEBUG("ObIndexBlockRowScanner::advance_to_border", K(found_pos), K(is_range_end),
                 K_(current), K_(start), K_(end), K(limit_idx), K(is_range_end));
      if (found_pos >= current_) {
        current_ = found_pos + 1;
        if (OB_FAIL(get_cur_row_id_range(cs_range))) {
          LOG_WARN("Failed to get cur row id range", K(ret), K(rowkey), K(current_), K(start_), K(end_),
                   K(limit_idx), K(is_range_end), KPC(idx_data_header_));
        }
      }
    } else {
      // found_pos is safe to skip.
      int64_t found_pos = start_found - idx_data_header_->rowkey_array_ + 1;
      // found_pos != start_, there is no need to check is_range_end.
      if (found_pos <= current_ + 1) {
        current_ = found_pos - 1;
        if (OB_FAIL(get_cur_row_id_range(cs_range))) {
          LOG_WARN("Failed to get cur row id range", K(ret), K(rowkey), K(current_), K(start_),
                    K(end_), K(limit_idx), K(is_range_end));
        }
      }
    }
  }
  LOG_DEBUG("ObIndexBlockRowScanner::advance_to_border", K(limit_idx), K(start_), K(end_), K(current_));
  return ret;
}

int ObIndexBlockRowScanner::find_out_rows(
    const int32_t range_idx,
    int64_t &found_idx)
{
  int ret = OB_SUCCESS;
  found_idx = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K_(is_inited));
  } else if (range_idx == range_idx_ && !end_of_block()) {
    const int64_t start_idx = current_;
    const int64_t end_idx = is_reverse_scan_ ? start_ : end_;
    for (int64_t i = start_idx; OB_SUCC(ret) && (i * step_) <= (end_idx * step_); i += step_) {
      const ObIndexBlockRowHeader *idx_row_header = nullptr;
      if (OB_FAIL(get_idx_row_header_in_target_idx(idx_row_header, i))) {
        LOG_WARN("Failed to get idx row header", K(i));
      } else if (idx_row_header->has_lob_out_row()) {
        found_idx = i;
        break;
      }
    }
  }
  LOG_DEBUG("ObIndexBlockRowScanner::find_out_rows", K(range_idx), K_(range_idx), K_(current), K(start_),
             K(end_), K(step_));
  return ret;
}

int ObIndexBlockRowScanner::find_out_rows_from_start_to_end(
    const int32_t range_idx,
    const ObCSRowId start_row_id,
    bool &is_certain,
    int64_t &found_idx)
{
  int ret = OB_SUCCESS;
  found_idx = -1;
  is_certain = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K_(is_inited));
  } else if (range_idx == range_idx_) {
    const int64_t start_idx = is_reverse_scan_ ? end_ : start_;
    const int64_t end_idx = is_reverse_scan_ ? start_ : end_;
    bool meet_start_row_id = false;
    for (int64_t i = start_idx; OB_SUCC(ret) && (i * step_) <= (end_idx * step_); i += step_) {
      const ObIndexBlockRowHeader *idx_row_header = nullptr;
      if (OB_FAIL(get_idx_row_header_in_target_idx(idx_row_header, i))) {
        LOG_WARN("Failed to get idx row header", K(i));
      }
      if (OB_SUCC(ret)) {
        if (!meet_start_row_id) {
          ObCSRowId cur_start_row_id = idx_row_parser_.get_row_offset() - idx_row_header->get_row_count() + 1;
          ObCSRowId cur_end_row_id = idx_row_parser_.get_row_offset();
          if (idx_row_header->is_data_block()) {
            cur_start_row_id += parent_row_range_.start_row_id_;
            cur_end_row_id += parent_row_range_.start_row_id_;
          }
          meet_start_row_id = (start_row_id >= cur_start_row_id && start_row_id <= cur_end_row_id);
        }
        if (meet_start_row_id && idx_row_header->has_lob_out_row()) {
          if ((i * step_) >= (current_ * step_)) {
            found_idx = i;
          } else {
            is_certain = false;
          }
          break;
        }
      }
    }
  } else {
    is_certain = false;
  }
  return ret;
}

bool ObIndexBlockRowScanner::is_in_border()
{
  bool in_border = false;
  if (!is_reverse_scan_) {
    in_border = is_right_border_ && current_ == end_;
  } else {
    in_border = is_left_border_ && current_ == start_;
  }
  return in_border;
}

void ObIndexBlockRowScanner::switch_context(const ObSSTable &sstable, const ObStorageDatumUtils &datum_utils)
{
  nested_offset_ = sstable.get_macro_offset();
  datum_utils_ = &datum_utils;
  is_normal_cg_ = sstable.is_normal_cg_sstable();
}

int ObIndexBlockRowScanner::init_datum_row()
{
  int ret = OB_SUCCESS;
  if (nullptr != datum_row_ && datum_row_->is_valid()) {
    // row allocated
  } else if (nullptr != datum_row_) {
     datum_row_->~ObDatumRow();
     allocator_->free(datum_row_);
     datum_row_ = nullptr;
  }
  if (nullptr == datum_row_) {
    int64_t request_cnt = datum_utils_->get_rowkey_count() + 1;
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObDatumRow)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory for datum row", K(ret));
    } else if (FALSE_IT(datum_row_ = new (buf) ObDatumRow())) {
    } else if (OB_FAIL(datum_row_->init(*allocator_, request_cnt))) {
      LOG_WARN("Fail to init datum row", K(ret), K(request_cnt));
    }

    if (OB_FAIL(ret) && nullptr != buf) {
      allocator_->free(buf);
      datum_row_ = nullptr;
    }
  }
  return ret;
}

int ObIndexBlockRowScanner::read_curr_idx_row(const ObIndexBlockRowHeader *&idx_row_header, const ObDatumRowkey *&endkey)
{
  int ret = OB_SUCCESS;
  idx_row_header = nullptr;
  const int64_t rowkey_column_count = datum_utils_->get_rowkey_count();
  idx_row_parser_.reset();
  if (IndexFormat::TRANSFORMED == index_format_) {
    const char *idx_data_buf = nullptr;
    int64_t idx_data_len = 0;
    if (OB_FAIL(idx_data_header_->get_index_data(current_, idx_data_buf, idx_data_len))) {
      LOG_WARN("Fail to get index data", K(ret), K_(current), KPC_(idx_data_header));
    } else if (OB_FAIL(idx_row_parser_.init(idx_data_buf, idx_data_len))) {
      LOG_WARN("Fail to parse index block row", K(ret), K_(current), KPC(idx_data_header_));
    } else if (OB_FAIL(idx_row_parser_.get_header(idx_row_header))) {
      LOG_WARN("Fail to get index block row header", K(ret));
    } else {
      endkey = &idx_data_header_->rowkey_array_[current_];
    }
  } else if (IndexFormat::RAW_DATA == index_format_) {
    endkey_.reset();
    if (OB_ISNULL(datum_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected null pointer to index row", K(ret));
    } else if (OB_FAIL(micro_reader_->get_row(current_, *datum_row_))) {
      LOG_WARN("Fail to read index row from block", K(ret), K(current_));
    } else if (OB_FAIL(idx_row_parser_.init(rowkey_column_count, *datum_row_))) {
      LOG_WARN("Fail to parser index block row", K(ret), K_(datum_row), K(rowkey_column_count));
    } else if (OB_FAIL(idx_row_parser_.get_header(idx_row_header))) {
      LOG_WARN("Fail to get index block row header", K(ret));
    } else if (OB_FAIL(endkey_.assign(datum_row_->storage_datums_, rowkey_column_count))) {
      LOG_WARN("Fail to assign storage datum to endkey", K(ret), KPC(datum_row_), K(rowkey_column_count));
    } else {
      endkey = &endkey_;
    }
  } else if (IndexFormat::BLOCK_TREE == index_format_) {
    if (OB_ISNULL(block_meta_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block meta iterator is null", K(ret));
    } else if (OB_FAIL(block_meta_tree_->get_index_block_row_header(current_, idx_row_header, endkey))) {
      LOG_WARN("get index block row header failed", K(ret), K(current_));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported index format", K(ret), K(index_format_));
  }
  return ret;
}

int ObIndexBlockRowScanner::get_next_idx_row(ObMicroIndexInfo &idx_block_row)
{
  int ret = OB_SUCCESS;
  const ObDatumRowkey *endkey = nullptr;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  const ObIndexBlockRowMinorMetaInfo *idx_minor_info = nullptr;
  const char *idx_data_buf = nullptr;
  const char *agg_row_buf = nullptr;
  int64_t agg_buf_size = 0;
  if (OB_FAIL(read_curr_idx_row(idx_row_header, endkey))) {
    LOG_WARN("Fail to read currend index row", K(ret), K_(index_format), K_(current));
  } else if (OB_UNLIKELY(nullptr == idx_row_header || nullptr == endkey)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index block row header/endkey", K(ret),
             K(index_format_), KP(idx_row_header), KP(endkey));
  } else if (idx_row_header->is_data_index() && !idx_row_header->is_major_node()) {
    if (OB_FAIL(idx_row_parser_.get_minor_meta(idx_minor_info))) {
      LOG_WARN("Fail to get minor meta info", K(ret));
    }
  } else if (!idx_row_header->is_major_node() || !idx_row_header->is_pre_aggregated()) {
    // Do not have aggregate data
  } else if (OB_FAIL(idx_row_parser_.get_agg_row(agg_row_buf, agg_buf_size))) {
    LOG_WARN("Fail to get aggregate", K(ret));
  }

  if (OB_SUCC(ret)) {
    idx_block_row.flag_ = 0;
    idx_block_row.endkey_ = endkey;
    idx_block_row.row_header_ = idx_row_header;
    idx_block_row.minor_meta_info_ = idx_minor_info;
    idx_block_row.is_get_ = is_get_;
    idx_block_row.is_left_border_ = is_left_border_ && current_ == start_;
    idx_block_row.is_right_border_ = is_right_border_ && current_ == end_;
    idx_block_row.copy_lob_out_row_flag();
    current_ += step_;
    idx_block_row.range_idx_ = range_idx_;
    idx_block_row.query_range_ = query_range_;
    idx_block_row.parent_macro_id_ = macro_id_;
    idx_block_row.nested_offset_ = nested_offset_;
    idx_block_row.agg_row_buf_ = agg_row_buf;
    idx_block_row.agg_buf_size_ = agg_buf_size;
    if (is_normal_cg_) {
      idx_block_row.cs_row_range_.start_row_id_ = idx_block_row.endkey_->datums_[0].get_int() - idx_block_row.get_row_count() + 1;
      idx_block_row.cs_row_range_.end_row_id_ = idx_block_row.endkey_->datums_[0].get_int();
      idx_block_row.set_filter_constant_type(filter_constant_type_);
    } else {
      idx_block_row.cs_row_range_.start_row_id_ = idx_row_parser_.get_row_offset() - idx_block_row.get_row_count() + 1;
      idx_block_row.cs_row_range_.end_row_id_ = idx_row_parser_.get_row_offset();
    }
    if (idx_block_row.is_data_block()) {
      idx_block_row.cs_row_range_.start_row_id_ += parent_row_range_.start_row_id_;
      idx_block_row.cs_row_range_.end_row_id_ += parent_row_range_.start_row_id_;
    }
  }
  LOG_DEBUG("Get next index block row", K(ret), K_(current), K_(start), K_(end), K(idx_block_row), KP(this), K(endkey_));
  return ret;
}

void ObIndexBlockRowScanner::skip_index_rows()
{
  for (; rowkey_begin_idx_ < rowkey_end_idx_; ++rowkey_begin_idx_) {
    if (!rows_info_->is_row_skipped(rowkey_begin_idx_)) {
      break;
    }
  }
}

int ObIndexBlockRowScanner::find_rowkeys_belong_to_same_idx_row(int64_t &rowkey_idx)
{
  int ret = OB_SUCCESS;
  bool is_decided = false;
  const ObDatumRowkey *cur_rowkey = nullptr;
  if (IndexFormat::TRANSFORMED == index_format_) {
    cur_rowkey = idx_data_header_->rowkey_array_ + current_;
  } else if (IndexFormat::BLOCK_TREE == index_format_) {
    cur_rowkey = block_meta_tree_->get_rowkey(current_);
  }
  for (; OB_SUCC(ret) && rowkey_begin_idx_ < rowkey_end_idx_; ++rowkey_begin_idx_) {
    if (rows_info_->is_row_skipped(rowkey_begin_idx_)) {
      continue;
    }
    const ObDatumRowkey &rowkey = rows_info_->get_rowkey(rowkey_begin_idx_);
    int32_t cmp_ret = 0;
    if (nullptr != cur_rowkey) {
      if (OB_FAIL(rowkey.compare(*cur_rowkey, *datum_utils_, cmp_ret, false))) {
        LOG_WARN("Failed to compare rowkey", K(ret), K(rowkey), KPC(cur_rowkey));
      }
    } else if (OB_FAIL(micro_reader_->compare_rowkey(rowkey, current_, cmp_ret))) {
      LOG_WARN("Failed to compare rowkey", K(ret), K(rowkey));
    } else {
      cmp_ret = -cmp_ret;
    }

    if (OB_FAIL(ret)) {
    } else if (cmp_ret > 0) {
      rowkey_idx = rowkey_begin_idx_;
      is_decided = true;
      break;
    } else if (cmp_ret == 0) {
      rowkey_idx = rowkey_begin_idx_ + 1;
      is_decided = true;
      break;
    }
  }
  if (!is_decided) {
    rowkey_idx = rowkey_begin_idx_;
  }
  return ret;
}

int ObIndexBlockRowScanner::skip_to_next_valid_position(ObMicroIndexInfo &idx_block_row)
{
  int ret = OB_SUCCESS;
  skip_index_rows();
  if (rowkey_begin_idx_ == rowkey_end_idx_) {
    ret = OB_ITER_END;
  } else if (IndexFormat::TRANSFORMED == index_format_) {
    const ObDatumRowkey &rowkey = rows_info_->get_rowkey(rowkey_begin_idx_);
    ObDatumComparor<ObDatumRowkey> cmp(*datum_utils_, ret, false, true, false);
    const ObDatumRowkey *first = idx_data_header_->rowkey_array_ + current_;
    const ObDatumRowkey *last = idx_data_header_->rowkey_array_ + end_ + 1;
    const ObDatumRowkey *found = std::lower_bound(first, last, rowkey, cmp);
    if (OB_FAIL(ret)) {
      LOG_WARN("Failed to get lower bound of rowkey", K(ret), K(rowkey), KPC_(idx_data_header));
    } else if (found == last) {
      ret = OB_ITER_END;
    } else {
      current_ = found - idx_data_header_->rowkey_array_;
    }
  } else if (IndexFormat::BLOCK_TREE == index_format_) {
    if (OB_FAIL(block_meta_tree_->skip_to_next_valid_position(rows_info_->get_rowkey(rowkey_begin_idx_),
                                                              *datum_utils_,
                                                              current_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("Failed to skip to next valid position in block meta tree", K(ret), K_(current), K_(rowkey_begin_idx),
                 KPC_(idx_data_header));
      }
    }
  } else if (IndexFormat::RAW_DATA == index_format_) {
    bool equal = false;
    if (OB_FAIL(micro_reader_->find_bound(rows_info_->get_rowkey(rowkey_begin_idx_), true, current_, current_, equal))) {
      LOG_WARN("Failed to skip to next valid position in micro block reader", K(ret), K_(current), K_(rowkey_begin_idx),
               KPC_(idx_data_header));
    } else if (current_ == (end_ + 1)) {
      ret = OB_ITER_END;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected index format", K_(index_format));
  }
  if (OB_SUCC(ret)) {
    idx_block_row.rows_info_ = rows_info_;
    idx_block_row.rowkey_begin_idx_ = rowkey_begin_idx_;
    if (OB_FAIL(find_rowkeys_belong_to_same_idx_row(idx_block_row.rowkey_end_idx_))) {
      LOG_WARN("Failed to find rowkeys belong to same index row", K(ret), K_(current), KPC_(idx_data_header));
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
