/**
 * Copyright (c) 2023 OceanBase
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
#include "storage/blocksstable/index_block/ob_agg_row_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_aggregator.h"

namespace oceanbase
{
namespace  blocksstable
{

ObAggRowHeader::ObAggRowHeader()
  :version_(AGG_ROW_HEADER_VERSION_3),
   length_(0),
   agg_col_cnt_(0),
   pack_(0)
   {}

ObAggRowWriter::ObAggRowWriter()
  : is_inited_(false),
    agg_data_(nullptr),
    column_count_(0),
    col_idx_count_(0),
    col_meta_list_(),
    header_(),
    row_helper_(),
    major_working_cluster_version_(0)
    {}

ObAggRowWriter::~ObAggRowWriter()
{
}

void ObAggRowWriter::reset()
{
  new (this) ObAggRowWriter();
}

int ObAggRowWriter::init(const ObIArray<ObSkipIndexColMeta> &agg_col_arr,
                         const ObSkipIndexAggResult &agg_data,
                         const int64_t major_working_cluster_version,
                         ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("agg row writer inited twice", K(ret));
  } else if (OB_UNLIKELY(agg_col_arr.count() != agg_data.get_agg_col_cnt() || !agg_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count not match", K(ret), K(agg_col_arr), K(agg_data));
  } else if (FALSE_IT(agg_data_ = &agg_data)) {
  } else if (FALSE_IT(major_working_cluster_version_ = major_working_cluster_version)) {
  } else if (OB_FAIL(sort_metas(agg_col_arr, allocator))) {
    LOG_WARN("failed to sort agg col metas", K(ret));
  } else if (OB_FAIL(calc_serialize_agg_buf_size())) {
    LOG_WARN("failed to calc estimate data size", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAggRowWriter::sort_metas(const ObIArray<ObSkipIndexColMeta> &agg_col_arr,
                               ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  column_count_ = agg_col_arr.count();
  col_meta_list_.clear();
  col_meta_list_.set_allocator(&allocator);
  if (OB_FAIL(col_meta_list_.reserve(column_count_))) {
    LOG_WARN("failed to reserve col meta list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; ++i) {
      if (OB_FAIL(col_meta_list_.push_back({agg_col_arr.at(i), i}))) {
        LOG_WARN("failed to push back col meta", K(ret), K(i));
      }
    }
    lib::ob_sort(col_meta_list_.begin(), col_meta_list_.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; ++i) {
      LOG_DEBUG("sort", K(i), K(col_meta_list_.at(i).first), K(col_meta_list_.at(i).second));
    }
  }
  return ret;
}

int ObAggRowWriter::calc_serialize_agg_buf_size()
{
  int ret = OB_SUCCESS;
  int64_t agg_header_size = 0;
  int64_t agg_data_size = 0;
  const bool enable_store_prefix = enable_skip_index_min_max_prefix(major_working_cluster_version_);
  header_.version_ = enable_revise_max_prefix(major_working_cluster_version_)
                     ? ObAggRowHeader::AGG_ROW_HEADER_VERSION_3
                     : (enable_store_prefix ? ObAggRowHeader::AGG_ROW_HEADER_VERSION_2 : ObAggRowHeader::AGG_ROW_HEADER_VERSION);
  header_.pack_ = 0;
  header_.agg_col_idx_size_ = 0;
  header_.bitmap_size_ = ObAggRowHeader::AGG_COL_TYPE_BITMAP_SIZE;
  uint32_t max_col_idx = col_meta_list_.at(column_count_ - 1).first.col_idx_;
  do {
    ++header_.agg_col_idx_size_;
    max_col_idx >>= 8;
  } while(max_col_idx != 0);

  col_idx_count_ = 0;
  header_.cell_off_size_ = 1; // default use 1 byte to save offset
  header_.agg_col_idx_off_size_ = 1;
  int64_t stored_col_cnt = 0;
  for (int64_t i = 0; i < column_count_; /*++i*/) {
    int64_t start = i, end = i;
    int64_t cur_cell_size = 0;
    int64_t nop_count = 0;
    const int64_t cur_col_idx = col_meta_list_.at(start).first.col_idx_;
    while (end < column_count_
        && cur_col_idx == col_meta_list_.at(end).first.col_idx_) {
      const ObStorageDatum &datum = agg_data_->get_agg_datum_row().storage_datums_[col_meta_list_.at(end).second];
      ++end;
      if (datum.is_nop_value() || datum.is_null()) {
        ++nop_count;
      } else {
        cur_cell_size += datum.len_;
      }
    }
    cur_cell_size += enable_store_prefix
        ? (2 * ObAggRowHeader::AGG_COL_TYPE_BITMAP_SIZE) : ObAggRowHeader::AGG_COL_TYPE_BITMAP_SIZE;
    int64_t cur_stored_col_cnt = end - start - nop_count;
    if (cur_stored_col_cnt > 0) {
      ++cur_stored_col_cnt; // reserve one more column to save cell size
    }
    if (header_.cell_off_size_ == 1 && cur_cell_size + cur_stored_col_cnt > UINT8_MAX) {
      header_.cell_off_size_ = 2;
    }
    ++col_idx_count_;
    agg_data_size += cur_cell_size;
    stored_col_cnt += cur_stored_col_cnt;
    i = end; // start next loop
  }
  agg_data_size += stored_col_cnt * header_.cell_off_size_;
  agg_header_size = sizeof(ObAggRowHeader) + col_idx_count_ * header_.agg_col_idx_size_
                    + col_idx_count_ * header_.agg_col_idx_off_size_;
  if (agg_data_size + agg_header_size > UINT8_MAX) {
    header_.agg_col_idx_off_size_ = 2;
    // We have to update agg header size, because agg_col_idx_off_size has changed.
    agg_header_size = sizeof(ObAggRowHeader) + col_idx_count_ * header_.agg_col_idx_size_
                      + col_idx_count_ * header_.agg_col_idx_off_size_;
    // We don't support larger skip index for now.
    if (OB_UNLIKELY(agg_data_size + agg_header_size > UINT16_MAX)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("fail to calculate serialize agg buf size, not support larger skip index",
               K(ret), K(header_), K(agg_data_size), K(agg_header_size));
    }
  }

  header_.agg_col_cnt_ = col_idx_count_;
  header_.length_ = agg_data_size + agg_header_size;
  return ret;
}


int ObAggRowWriter::write_cell(
    int64_t start,
    int64_t end,
    int64_t nop_count,
    char *buf,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t orig_pos = pos;
  char *cell_buf = buf + pos;
  char *bitmap = cell_buf;
  const bool enable_store_prefix = enable_skip_index_min_max_prefix(major_working_cluster_version_);
  const int64_t total_bitmap_size = enable_store_prefix ? header_.bitmap_size_ * 2 : header_.bitmap_size_;
  if (OB_UNLIKELY(start + nop_count > end || nullptr == agg_data_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(start), K(end), K(nop_count), KP_(agg_data));
  } else if (OB_FAIL(row_helper_.col_bitmap_gen_.init(buf + pos, header_.bitmap_size_))) {
    LOG_WARN("failed to init bitmap", K(ret));
  } else if (FALSE_IT(pos += total_bitmap_size)) {
  } else if (OB_FAIL(row_helper_.col_off_gen_.init(buf + pos, header_.cell_off_size_))) {
    LOG_WARN("failed to init col off arr", K(ret));
  } else {
    ObIIntegerArray &col_bitmap = row_helper_.col_bitmap_gen_.get_array();
    col_bitmap.set(ObAggRowHeader::TYPE_BITMAP_IDX, 0);
    if (enable_store_prefix) {
      col_bitmap.set(ObAggRowHeader::PREFIX_BITMAP_IDX, 0);
    }
    ObIIntegerArray &col_off_arr = row_helper_.col_off_gen_.get_array();
    int64_t stored_col_cnt = end - start - nop_count;
    if (stored_col_cnt > 0) {
      ++stored_col_cnt; // reserve one more column to save cell size
    }
    pos += stored_col_cnt * header_.cell_off_size_;
    int64_t idx = 0;
    int64_t cur = start;
    while (OB_SUCC(ret) && idx < stored_col_cnt - 1 && cur < end) {
      const int64_t agg_col_idx = col_meta_list_.at(cur).second;
      const ObStorageDatum &datum = agg_data_->get_agg_datum_row().storage_datums_[agg_col_idx];
      const bool is_min_max_prefix = agg_data_->get_agg_attrs().at(agg_col_idx).is_min_max_prefix_;
      const uint8_t type = col_meta_list_.at(cur).first.col_type_;
      ++cur;
      if (datum.is_nop_value() || datum.is_null()) {
        continue; // skip nop value
      }
      int64_t type_bitmap_val = col_bitmap.at(ObAggRowHeader::TYPE_BITMAP_IDX);
      type_bitmap_val = type_bitmap_val | (1L << type);
      col_bitmap.set(ObAggRowHeader::TYPE_BITMAP_IDX, type_bitmap_val);

      if (enable_store_prefix && is_min_max_prefix
          && (type == ObSkipIndexColType::SK_IDX_MIN || type == ObSkipIndexColType::SK_IDX_MAX)) {
        int64_t prefix_bitmap_val = col_bitmap.at(ObAggRowHeader::PREFIX_BITMAP_IDX);
        prefix_bitmap_val = prefix_bitmap_val | (1L << type);
        col_bitmap.set(ObAggRowHeader::PREFIX_BITMAP_IDX, prefix_bitmap_val);
      }

      if (OB_ISNULL(datum.ptr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null datum", K(ret), K(datum));
      } else {
        col_off_arr.set(idx, pos - orig_pos);
        MEMCPY(buf + pos, datum.ptr_, datum.len_); // copy data
        LOG_DEBUG("write cell", K(idx), K(datum), K(pos), K(type_bitmap_val), K(start), K(end));
        pos += datum.len_;
      }
      ++idx;
    }
    if (OB_SUCC(ret) && stored_col_cnt > 0) {
      LOG_DEBUG("write cell(reserved)", K(idx), K(pos), K(orig_pos), K(header_));
      col_off_arr.set(stored_col_cnt - 1, pos - orig_pos); // cell end
    }
  }
  return ret;
}

int ObAggRowWriter::write_agg_data(char *buf, const int64_t buf_size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t orig_pos = pos;
  const int64_t col_idx_arr_size = col_idx_count_ * header_.agg_col_idx_size_;
  const int64_t col_idx_off_arr_size = col_idx_count_ * header_.agg_col_idx_off_size_;
  ObAggRowHeader *header = reinterpret_cast<ObAggRowHeader *>(buf + pos);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(buf_size < pos + get_serialize_data_size())) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf not enough, cannot write data", K(ret), K(buf_size), K(pos), K_(header));
  } else if (FALSE_IT(pos += sizeof(ObAggRowHeader))) {
  } else if (OB_FAIL(row_helper_.col_idx_gen_.init(buf + pos, header_.agg_col_idx_size_))) {
    LOG_WARN("failed to init col idx arr", K(ret), K_(header));
  } else if (FALSE_IT(pos += col_idx_arr_size)) {
  } else if (OB_FAIL(row_helper_.col_idx_off_gen_.init(buf + pos, header_.agg_col_idx_off_size_))) {
    LOG_WARN("failed to init col idx off arr", K(ret), K_(header));
  } else {
    ObIIntegerArray &col_idx_arr = row_helper_.col_idx_gen_.get_array();
    ObIIntegerArray &col_idx_off_arr = row_helper_.col_idx_off_gen_.get_array();
    pos += col_idx_off_arr_size;

    int64_t start = 0, end = 0; // include start, not include end
    int64_t cur_col_idx = 0;
    int64_t nop_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i <= column_count_ && start < column_count_; /*++i*/) {
      if (i < column_count_
          && col_meta_list_.at(i).first.col_idx_ == col_meta_list_.at(start).first.col_idx_) {
        const ObStorageDatum &datum = agg_data_->get_agg_datum_row().storage_datums_[col_meta_list_.at(i).second];
        if (datum.is_nop_value() || datum.is_null()) {
          ++nop_count;
        }
        ++i;
        continue;
      }
      end = i;
      col_idx_arr.set(cur_col_idx, col_meta_list_.at(start).first.col_idx_);
      col_idx_off_arr.set(cur_col_idx, pos - orig_pos);
      if (OB_FAIL(write_cell(start, end, nop_count, buf, pos))) {
        LOG_WARN("failed to write an agg cell", K(ret), K(cur_col_idx));
      } else {
        ++cur_col_idx;
        start = end;
        nop_count = 0;
        i = start; // next loop will start from i
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing.
    } else if (OB_UNLIKELY(!header_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid agg row header", K(ret), K(header_), K(pos), K(orig_pos));
    } else if (OB_UNLIKELY(pos - orig_pos != get_serialize_data_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to write agg data, unexpected data size", K(ret), K(header_), K(pos), K(orig_pos));
    } else {
      *header = header_;
    }
  }
  return ret;
}


//============================= ObAggRowReader //=============================

ObAggRowReader::ObAggRowReader()
  :is_inited_(false),
   buf_(nullptr),
   buf_size_(0)
  {}

ObAggRowReader::~ObAggRowReader()
{
  reset();
}

void ObAggRowReader::reset()
{
  buf_ = nullptr;
  buf_size_ = 0;
  is_inited_ = false;
}

int ObAggRowReader::init(const char *buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("agg row reader inited twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == buf || buf_size <= 0 || buf_size < sizeof(ObAggRowHeader))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(inner_init(buf, buf_size))) {
    LOG_WARN("failed to inner init", K(ret));
  } else {
    buf_ = buf;
    buf_size_ = buf_size;
    is_inited_ = true;
  }
  return ret;
}

int ObAggRowReader::inner_init(const char *buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  header_ = reinterpret_cast<const ObAggRowHeader *>(buf);
  int64_t pos = sizeof(ObAggRowHeader);
  if (OB_UNLIKELY(!header_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid agg row header", K(ret), KPC_(header));
  } else {
    const int64_t col_idx_arr_size = header_->agg_col_idx_size_ * header_->agg_col_cnt_;
    const int64_t col_idx_off_arr_size = header_->agg_col_idx_off_size_ * header_->agg_col_cnt_;
    if (OB_UNLIKELY(buf_size < pos + col_idx_arr_size + col_idx_off_arr_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf size too small", K(ret), K(buf_size), KPC_(header), K(pos));
    } else if (OB_FAIL(row_helper_.col_idx_gen_.init(buf + pos, header_->agg_col_idx_size_))) {
      LOG_WARN("failed to init col idx arr", K(ret), KPC_(header));
    } else if (FALSE_IT(pos += col_idx_arr_size)) {
    } else if (OB_FAIL(row_helper_.col_idx_off_gen_.init(buf + pos, header_->agg_col_idx_off_size_))) {
      LOG_WARN("failed to init col idx off arr", K(ret), KPC_(header));
    } else {
      header_size_ = pos + col_idx_off_arr_size;
    }
  }
  return ret;
}

int ObAggRowReader::read(const ObSkipIndexColMeta &meta, ObDatum &datum)
{
  bool is_prefix = false;
  return read(meta, datum, is_prefix);
}

int ObAggRowReader::read(const ObSkipIndexColMeta &meta, ObDatum &datum, bool &is_prefix)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  datum.set_null();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("agg row reader not inited", K(ret));
  } else if (OB_UNLIKELY(!meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid col meta", K(ret), K(meta));
  } else if (OB_FAIL(binary_search_col(meta.col_idx_, pos))) {
    LOG_WARN("failed to find column idx", K(ret), K(meta));
  } else if (!pos) {
    LOG_DEBUG("not aggregated", K(ret), K(meta));
  } else if (OB_FAIL(find_col(pos, meta.col_type_, datum, is_prefix))) {
    LOG_WARN("failed to find agg data", K(ret), K(meta));
  }
  return ret;
}

int ObAggRowReader::binary_search_col(const int64_t col_idx, int64_t &pos)
{
  int ret = OB_SUCCESS;
  pos = 0;
  ObIIntegerArray &col_idx_arr = row_helper_.col_idx_gen_.get_array();
  ObIIntegerArray &col_idx_off_arr = row_helper_.col_idx_off_gen_.get_array();
  const int64_t column_count = header_->agg_col_cnt_;
  const int64_t idx = col_idx_arr.lower_bound(0, column_count, col_idx);
  if (idx >= 0 && idx < column_count && col_idx_arr.at(idx) == col_idx) {
    pos = col_idx_off_arr.at(idx);
  }
  return ret;
}

int ObAggRowReader::find_col(const int64_t pos, const int64_t type, ObDatum &datum, bool &is_prefix)
{
  int ret = OB_SUCCESS;
  const char *cell_buf = 0;
  bool found = false;
  int64_t col_off = 0;
  int64_t col_len = 0;
  datum.reset();
  if (OB_UNLIKELY(!pos || pos + header_->bitmap_size_ > buf_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pos to read", K(ret), K(pos), KPC_(header), K_(buf_size));
  } else if (FALSE_IT(cell_buf = buf_ + pos)) {
  } else if (OB_FAIL(read_cell(cell_buf, buf_size_ - pos, type, found, col_off, col_len, is_prefix))) {
    LOG_WARN("failed to locate col in cell", K(ret));
  } else if (!found) {
    datum.set_null();
  } else {
    datum.ptr_ = cell_buf + col_off;
    datum.len_ = col_len;
  }
  return ret;
}

int ObAggRowReader::read_cell(
    const char *cell_buf, const int64_t buf_size, const int64_t type,
    bool &found, int64_t &col_off, int64_t &col_len, bool &is_prefix)
{
  int ret = OB_SUCCESS;
  found = false;
  is_prefix = false;
  col_off = 0;
  col_len = 0;
  int64_t cell_type_bitmap_val = 0;
  int64_t tar_mask = 1L << type;
  int64_t cell_size = 0;
  const int64_t bitmap_arr_size = has_cell_prefix_bitmap() ? (2 * header_->bitmap_size_) : header_->bitmap_size_;
  if (OB_UNLIKELY(buf_size < bitmap_arr_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected buf size", K(ret), K(buf_size), KPC_(header));
  } else if (OB_FAIL(row_helper_.col_bitmap_gen_.init(cell_buf, header_->bitmap_size_))) {
    LOG_WARN("failed to init bitmap", K(ret));
  } else if (FALSE_IT(cell_type_bitmap_val = row_helper_.col_bitmap_gen_.get_array().at(ObAggRowHeader::TYPE_BITMAP_IDX))) {
  } else if (!(cell_type_bitmap_val & tar_mask)) {
    found = false;
  } else if (OB_UNLIKELY(buf_size < bitmap_arr_size + header_->cell_off_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected buf size when bitmap matches", K(ret), K(buf_size), KPC_(header));
  } else if (OB_FAIL(row_helper_.col_off_gen_.init(
      cell_buf + bitmap_arr_size, header_->cell_off_size_))) {
    LOG_WARN("failed to init col off gen", K(ret));
  } else {
    found = true;
    ObIIntegerArray &col_off_arr = row_helper_.col_off_gen_.get_array();
    int64_t pre_cnt = 0;
    int64_t n = cell_type_bitmap_val & (tar_mask - 1); // reserve the bitmap of columns stored before the target type
    while (n) {
      ++pre_cnt;
      n = n & (n - 1); // quickly remove the last 1 of n
    }
    col_off = col_off_arr.at(pre_cnt);
    col_len = col_off_arr.at(pre_cnt + 1) - col_off;
    if (has_cell_prefix_bitmap()) {
      int64_t cell_prefix_bitmap_val = row_helper_.col_bitmap_gen_.get_array().at(ObAggRowHeader::PREFIX_BITMAP_IDX);
      is_prefix = cell_prefix_bitmap_val & tar_mask;
    }
    LOG_DEBUG("read cell", K(ret), K(pre_cnt), K(col_off), K(col_len));
  }
  return ret;
}


} // end namespace blocksstable
} // end namespace oceanbase