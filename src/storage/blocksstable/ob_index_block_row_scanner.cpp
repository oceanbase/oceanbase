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
#include "share/schema/ob_column_schema.h"
#include "ob_index_block_row_scanner.h"
#include "ob_index_block_row_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase
{
using namespace storage;
namespace blocksstable
{

int ObIndexBlockDataHeader::get_index_data(const int64_t row_idx, const char *&index_ptr) const
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
    char *transform_buf,
    int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t row_cnt = 0;
  int64_t size_required = 0;
  ObArenaAllocator allocator;
  ObDatumRow row;
  ObIMicroBlockReader *micro_reader = nullptr;
  ObIndexBlockDataHeader *idx_header = nullptr;
  ObDatumRowkey *rowkey_arr = nullptr;
  ObStorageDatum *datum_buf = nullptr;
  char *data_buf = transform_buf;
  const ObMicroBlockHeader *micro_block_header =
      reinterpret_cast<const ObMicroBlockHeader *>(block_data.get_buf());
  int64_t col_cnt = micro_block_header->rowkey_column_count_ + 1;
  if (OB_UNLIKELY(!block_data.is_valid() || !micro_block_header->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(block_data), KPC(micro_block_header));
  } else if (OB_FAIL(get_reader(block_data.get_store_type(), micro_reader))) {
    LOG_WARN("Fail to set micro block reader", K(ret));
  } else if (OB_FAIL(micro_reader->init(block_data, nullptr))) {
    LOG_WARN("Fail to init micro block reader", K(ret), K(block_data));
  } else if (OB_FAIL(micro_reader->get_row_count(row_cnt))) {
    LOG_WARN("Fail to get row count", K(ret));
  } else if (FALSE_IT(size_required = get_transformed_block_mem_size(block_data))) {
  } else if (OB_UNLIKELY(buf_len < size_required) || OB_ISNULL(transform_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Extra buf in block data is invalid", K(ret), K(size_required), K(buf_len));
  } else if (OB_FAIL(row.init(allocator, col_cnt))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret), K(col_cnt));
  } else {
    idx_header = reinterpret_cast<ObIndexBlockDataHeader *>(data_buf);
    data_buf += sizeof(ObIndexBlockDataHeader);
    rowkey_arr = reinterpret_cast<ObDatumRowkey *>(data_buf);
    data_buf += sizeof(ObDatumRowkey) * row_cnt;
    datum_buf = reinterpret_cast<ObStorageDatum *> (data_buf);

    for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
      row.reuse();
      if (OB_FAIL(micro_reader->get_row(i, row))) {
        LOG_WARN("Fail to get row", K(ret));
      } else if (OB_FAIL(rowkey_arr[i].assign(datum_buf + i * col_cnt, col_cnt - 1))) {
        LOG_WARN("Fail to assign obj array to rowkey", K(ret), K(row));
      } else {
        ObStorageDatum *datum_arr = new (datum_buf + i * col_cnt) ObStorageDatum[col_cnt];
        for (int64_t j = 0; j < col_cnt; j++) {
          datum_arr[j] = row.storage_datums_[j];
        }
      }
    }

    if (OB_SUCC(ret)) {
      idx_header->row_cnt_ = row_cnt;
      idx_header->col_cnt_ = col_cnt;
      idx_header->rowkey_array_ = rowkey_arr;
      idx_header->datum_array_ = datum_buf;
      STORAGE_LOG(DEBUG, "chaser debug transfer index block", KPC(idx_header), K(block_data.get_store_type()));
    }
  }
  return ret;
}

// Re-transform the block with col meta array to refresh the pointers on transfomed block deep copy
// Maybe we shall re-calculate the relative offset of all pointers if better performace is required
int ObIndexBlockDataTransformer::update_index_block(
    const ObIndexBlockDataHeader &src_idx_header,
    const char *micro_data,
    const int64_t micro_data_size,
    char *transform_buf,
    int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const ObMicroBlockHeader *micro_block_header =
      reinterpret_cast<const ObMicroBlockHeader *>(micro_data);
  if (OB_UNLIKELY(!src_idx_header.is_valid() || micro_data_size < 0 || buf_len < 0)
      || OB_ISNULL(transform_buf)
      || OB_ISNULL(micro_block_header)
      || OB_UNLIKELY(!micro_block_header->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid src micro block data", K(ret), K(src_idx_header),
        KP(micro_data), K(micro_data_size), KP(transform_buf), K(buf_len));
  } else {
    ObMicroBlockData target_block(micro_data, micro_data_size);
    if (OB_FAIL(transform(target_block, transform_buf, buf_len))) {
      LOG_WARN("Fail to re transform index block data", K(ret));
    }
  }
  return ret;
}

int64_t ObIndexBlockDataTransformer::get_transformed_block_mem_size(
    const int64_t row_cnt,
    const int64_t idx_col_cnt)
{
  return sizeof(ObIndexBlockDataHeader)
      + row_cnt * sizeof(ObDatumRowkey)
      + row_cnt * sizeof(ObStorageDatum) * idx_col_cnt;
}

int64_t ObIndexBlockDataTransformer::get_transformed_block_mem_size(
    const ObMicroBlockData &index_block_data)
{
  const ObMicroBlockHeader *micro_header = index_block_data.get_micro_header();
  return get_transformed_block_mem_size(micro_header->row_count_, micro_header->column_count_);
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
    step_(1), range_idx_(0), nested_offset_(0), index_format_(IndexFormat::INVALID), is_get_(false),
    is_reverse_scan_(false), is_left_border_(false), is_right_border_(false), is_inited_(false)
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
}

void ObIndexBlockRowScanner::reset()
{
  query_range_ = nullptr;
  idx_data_header_ = nullptr;
  micro_reader_helper_.reset();
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
  index_format_ = IndexFormat::INVALID;
  is_get_ = false;
  is_reverse_scan_ = false;
  is_left_border_ = false;
  is_right_border_ = false;
  is_inited_ = false;
}

int ObIndexBlockRowScanner::init(
    const ObIArray<int32_t> &agg_projector,
    const ObIArray<share::schema::ObColumnSchemaV2> &agg_column_schema,
    const ObStorageDatumUtils &datum_utils,
    ObIAllocator &allocator,
    const common::ObQueryFlag &query_flag,
    const int64_t nested_offset)
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
    is_inited_ = true;
  }
  return ret;
}

int ObIndexBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &idx_block_data,
    const ObDatumRowkey &rowkey,
    const int64_t range_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !idx_block_data.is_valid() || !rowkey.is_valid()
      || !idx_block_data.is_index_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to open an index micro block", K(ret),
        K(macro_id), K(idx_block_data), K(rowkey));
  } else if (OB_FAIL(init_by_micro_data(idx_block_data))) {
    LOG_WARN("Fail to init scanner by micro data", K(ret), K(idx_block_data));
  } else if (OB_FAIL(locate_key(rowkey))) {
    LOG_WARN("Fail to locate rowkey", K(ret), K(idx_block_data), K(rowkey));
  } else {
    macro_id_ = macro_id;
    range_idx_ = range_idx;
    rowkey_ = &rowkey;
    is_get_ = true;
  }
  return ret;
}

int ObIndexBlockRowScanner::open(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &idx_block_data,
    const ObDatumRange &range,
    const int64_t range_idx,
    const bool is_left_border,
    const bool is_right_border)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || !idx_block_data.is_valid() || !range.is_valid()
      || !idx_block_data.is_index_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to open an index micro block", K(ret), K(idx_block_data), K(range));
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
    range_ = &range;
    is_get_ = false;
  }
  return ret;
}

int ObIndexBlockRowScanner::get_next(ObMicroIndexInfo &idx_block_row)
{
  idx_block_row.reset();
  const ObDatumRowkey *endkey = nullptr;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  const ObIndexBlockRowMinorMetaInfo *idx_minor_info = nullptr;
  const char *idx_data_buf = nullptr;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (end_of_block()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(read_curr_idx_row(idx_row_header, endkey))) {
    LOG_WARN("Fail to read currend index row", K(ret), K_(index_format), K_(current));
  } else if (OB_UNLIKELY(nullptr == idx_row_header || nullptr == endkey)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null index block row header/endkey", K(ret),
             K(index_format_), KP(idx_row_header), KP(endkey));
  } else if (idx_row_header->is_data_index() && !idx_row_header->is_major_node()) {
    if (OB_FAIL(idx_row_parser_.get_minor_meta(idx_minor_info))) {
      LOG_WARN("Fail to get minor meta info", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    idx_block_row.flag_ = 0;
    idx_block_row.endkey_ = endkey;
    idx_block_row.row_header_ = idx_row_header;
    idx_block_row.minor_meta_info_ = idx_minor_info;
    idx_block_row.is_get_ = is_get_;
    idx_block_row.is_left_border_ = is_left_border_ && current_ == start_;
    idx_block_row.is_right_border_ = is_right_border_ && current_ == end_;
    current_ += step_;
    idx_block_row.range_idx_ = range_idx_;
    idx_block_row.query_range_ = query_range_;
    idx_block_row.parent_macro_id_ = macro_id_;
    idx_block_row.nested_offset_ = nested_offset_;
  }
  LOG_DEBUG("Get next index block row", K(ret), K_(current), K_(start), K_(end), K(idx_block_row));
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
      if (range.get_border_flag().inclusive_end()) {
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
  }
  return ret;
}

void ObIndexBlockRowScanner::switch_context(const ObSSTable &sstable, const ObStorageDatumUtils &datum_utils)
{
  nested_offset_ = sstable.get_macro_offset();
  datum_utils_ = &datum_utils;
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
  if (IndexFormat::TRANSFORMED == index_format_) {
    const char *idx_data_buf = nullptr;
    if (OB_FAIL(idx_data_header_->get_index_data(current_, idx_data_buf))) {
      LOG_WARN("Fail to get index data", K(ret), K_(current), KPC_(idx_data_header));
    } else if (OB_FAIL(idx_row_parser_.init(idx_data_buf))) {
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

} // namespace blocksstable
} // namespace oceanbase
